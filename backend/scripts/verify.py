from shapely import wkt
from shapely.geometry import Point, LineString
from shapely.ops import unary_union
import pandas as pd
import re

def convert_geometries_to_wkt(gdf):
    gdf['wkt'] = gdf['geometry'].apply(lambda geom: wkt.dumps(geom))
    return gdf

async def verify_geometries_in_zones(gdf, zgdf, zone_type):
    if gdf.crs != zgdf.crs:
        gdf = gdf.to_crs(zgdf.crs)

    gdf = convert_geometries_to_wkt(gdf)
    zgdf = convert_geometries_to_wkt(zgdf)

    if zone_type == 'PA' or zone_type == 'PB':
        z_dict = {row['pcn_code']: wkt.loads(row['wkt']) for idx, row in zgdf.iterrows()}
    elif zone_type == 'SRO':
        z_dict = {row['zs_code']: (wkt.loads(row['wkt']), row['zs_nd_code']) for _, row in zgdf.iterrows()}
    elif zone_type == 'NRO':
        z_dict = {row['zn_code']: (wkt.loads(row['wkt']), row['zn_nd_code']) for _, row in zgdf.iterrows()}
    else:
        raise ValueError("Le paramètre 'zone_type' doit être 'PA', 'PB', 'SRO' ou 'NRO'.")

    not_in_zones = []
    nd_code_mismatch = []
    for _, row in gdf.iterrows():
        geom = wkt.loads(row['wkt'])
        if zone_type == 'PA' or zone_type == 'PB':
            z_geom = z_dict.get(row['pcn_code'])
            if z_geom and z_geom.contains(geom):
                continue
            else:
                not_in_zones.append(row['pcn_code'])
        else:
            nd_code = row['nd_code']
            z_data = None
            for z_code, (z_geom, z_nd_code) in z_dict.items():
                if z_nd_code == nd_code:
                    z_data = (z_geom, z_code)
                    break

            if not z_data:
                not_in_zones.append(nd_code)
                continue

            z_geom, z_code = z_data

            if not z_geom.contains(geom):
                not_in_zones.append(nd_code)
            elif nd_code != z_nd_code:
                nd_code_mismatch.append(nd_code)

    if not_in_zones:
        print(f"Le(s) {zone_type}(s) suivants n'appartiennent pas à la zone Z{zone_type} assignée :", not_in_zones)
    if nd_code_mismatch:
        print(f"Le(s) {zone_type}(s) suivants ont un nd_code différent de z_nd_code :", nd_code_mismatch)

    return not_in_zones, nd_code_mismatch

async def check_zp_intersections(zp_gdf, x):
    if zp_gdf.crs is None:
        raise ValueError(f"Le GeoDataFrame des Z{x} doit avoir un système de coordonnées (CRS) défini.")
    zp_gdf = convert_geometries_to_wkt(zp_gdf)
    intersecting_zp = []
    for i, row1 in zp_gdf.iterrows():
        for j, row2 in zp_gdf.iloc[i+1:].iterrows():  
            if row1.geometry.intersects(row2.geometry) and not row1.geometry.touches(row2.geometry):  
                if x == 'SRO': 
                    intersecting_zp.append((row1['zs_code'], row2['zs_code'])) 
                else:
                    intersecting_zp.append((row1['pcn_code'], row2['pcn_code']))
    if intersecting_zp:
        print(f"Les Z{x} suivants ont de vraies intersections :")
        for zp1, zp2 in intersecting_zp:
            print(f"- {zp1} intersecte avec {zp2}")
    return intersecting_zp

async def verify_zsro_in_zonenro(zsro_gdf, znro_gdf):
    if zsro_gdf.crs != znro_gdf.crs:
        zsro_gdf = zsro_gdf.to_crs(znro_gdf.crs)

    zsro_gdf = convert_geometries_to_wkt(zsro_gdf)
    znro_gdf = convert_geometries_to_wkt(znro_gdf)

    znro_dict = {row['zn_r3_code']: wkt.loads(row['wkt']) for idx, row in znro_gdf.iterrows()}

    zsro_not_in_zones = []

    for idx, row in zsro_gdf.iterrows():
        zsro_geom = wkt.loads(row['wkt'])
        zs_code = row['zs_code']
        znro_geom = znro_dict.get(row['zs_r3_code'])

        if znro_geom:
            intersection = znro_geom.intersection(zsro_geom)
            if not intersection.equals(zsro_geom):
                zsro_not_in_zones.append(zs_code)
        else:
            zsro_not_in_zones.append(zs_code)

    if zsro_not_in_zones:
        print("ZSRO n'appartiennent pas aux ZNRO assignées:")
        for zsro in zsro_not_in_zones:
            print(f"- {zsro}")
    return zsro_not_in_zones

async def detect_self_intersections_c(c_gdf, type):
    if c_gdf.crs is None:
        raise ValueError(f"Le GeoDataFrame des {type} doit avoir un système de coordonnées (CRS) défini.")

    self_intersecting_c = []
    code_attribute = 'cm_codeext' if type == 'CM' else 'cl_codeext'

    for i, row in c_gdf.iterrows():
        geom = row['geometry']
        if isinstance(geom, (LineString)):
            if geom.is_valid:
                if geom.is_simple:
                    continue
                else:
                    self_intersecting_c.append(row[code_attribute])
            else:
                self_intersecting_c.append(row[code_attribute])
                print(f"{type} {row[code_attribute]} a une géométrie invalide.")
        else:
            print(f"{type} {row[code_attribute]} n'est pas une géométrie de type LineString.")

    if self_intersecting_c:
        print(f"Les {type} suivants ont des auto-intersections :")
        for c in self_intersecting_c:
            print(f"- {c}")

    return self_intersecting_c

async def verify_c_intersections(c_di_gdf, support_gdf, pb_gdf, pa_gdf, sro_gdf, adresse_gdf, type):
    if c_di_gdf.crs is None:
        raise ValueError("Le GeoDataFrame des CB doit avoir un système de coordonnées (CRS) défini.")

    intersecting_c = []

    if type == 'CB':
        code_attribute = 'cl_codeext'
    elif type == 'CM':
        code_attribute = 'cm_codeext'
    else:
        raise ValueError(f"Type inconnu: {type}. Les types valides sont 'CB' et 'CM'.")

    for i, row1 in c_di_gdf.iterrows():
        for j, row2 in c_di_gdf.iloc[i+1:].iterrows():
            if row1.geometry.intersects(row2.geometry) and not row1.geometry.touches(row2.geometry):
                intersection_point = row1.geometry.intersection(row2.geometry)
                
                if not (
                    pb_gdf.geometry.touches(intersection_point).any() or
                    pa_gdf.geometry.touches(intersection_point).any() or
                    sro_gdf.geometry.touches(intersection_point).any() or
                    (support_gdf.geometry.contains(intersection_point).any() or support_gdf.geometry.touches(intersection_point).any()) or
                    adresse_gdf.geometry.touches(intersection_point).any()
                ):
                    intersecting_c.append((row1[code_attribute], row2[code_attribute]))

    if intersecting_c:
        print(f"Il y a des éléments de la couche {type} qui s'intersectent.")
        for c1, c2 in intersecting_c:
            print(f"- {c1} intersecte avec {c2}")

    return intersecting_c

async def verify_mic_pm(zsro_gdf):
    try:        
        sro_nv = zsro_gdf[zsro_gdf['pcn_umtot'] > 90]
        if not sro_nv.empty:
            print(f"Vous avez dépassé le nombre de  µm maximale par PM (90 µm)")
    except Exception as e:
        print(f"Erreur lors de la vérification des longueurs : {e}")

async def detect_cb_without_cm(cb_di_gdf, cm_di_gdf, support_gdf, pb_gdf, pa_gdf, sro_gdf):
    if cb_di_gdf.crs != cm_di_gdf.crs:
        cm_di_gdf = cm_di_gdf.to_crs(cb_di_gdf.crs)
    if cb_di_gdf.crs != support_gdf.crs:
        support_gdf = support_gdf.to_crs(cb_di_gdf.crs)
    if cb_di_gdf.crs != pb_gdf.crs:
        pb_gdf = pb_gdf.to_crs(cb_di_gdf.crs)
    if cb_di_gdf.crs != pa_gdf.crs:
        pa_gdf = pa_gdf.to_crs(cb_di_gdf.crs)
    if cb_di_gdf.crs != sro_gdf.crs:
        sro_gdf = sro_gdf.to_crs(cb_di_gdf.crs)

    cb_sans_cm = []

    for idx_cb, cb_row in cb_di_gdf.iterrows():
        cb_geom = cb_row.geometry

        match_found = any(cb_geom.equals(cm_geom) for cm_geom in cm_di_gdf.geometry)

        if not match_found:
            cm_union = unary_union(cm_di_gdf.geometry)
            cb_diff_cm = cb_geom.difference(cm_union)

            supports_in_cb = support_gdf[support_gdf.geometry.within(cb_geom)]
            pbs_in_cb = pb_gdf[pb_gdf.geometry.within(cb_geom)]
            pas_in_cb = pa_gdf[pa_gdf.geometry.within(cb_geom)]
            sros_in_cb = sro_gdf[sro_gdf.geometry.within(cb_geom)]

            all_elements_geom = unary_union(
                list(supports_in_cb.geometry) + 
                list(pbs_in_cb.geometry) + 
                list(pas_in_cb.geometry) + 
                list(sros_in_cb.geometry)
            )

            uncovered_area = cb_diff_cm.difference(all_elements_geom)

            if not uncovered_area.is_empty:
                cb_sans_cm.append(cb_row['cl_codeext'])

    if cb_sans_cm:
        print("Les CB suivants ne disposent d’aucun CM :")
        for cb in cb_sans_cm:
            print(f"- {cb}")

    return cb_sans_cm

async def check_column_duplicates(data, column_name, file_key):
    duplicates = data[data.duplicated(subset=[column_name], keep=False)]
    if not duplicates.empty:
        unique_duplicates = set(duplicates[column_name].tolist())
        print(f"Doublons trouvés dans le fichier {file_key} pour la colonne {column_name}:")
        print(list(unique_duplicates))

async def check_duplicates(dataframes):
    try:
        for file_key, data in dataframes:
            if file_key == "CB_DI" and 'cl_codeext' in data.columns:
                await check_column_duplicates(data, 'cl_codeext', file_key)
            if file_key == "CM_DI" and 'cm_codeext' in data.columns:
                await check_column_duplicates(data, 'cm_codeext', file_key)
            if file_key == "PB" and 'pcn_code' in data.columns:
                await check_column_duplicates(data, 'pcn_code', file_key)
            if file_key == "ADRESSE" and 'ad_code' in data.columns:
                await check_column_duplicates(data, 'ad_code', file_key)
            if file_key == "NRO" and 'nd_code' in data.columns:
                await check_column_duplicates(data, 'nd_code', file_key)
            if file_key == "PA" and 'pcn_code' in data.columns:
                await check_column_duplicates(data, 'pcn_code', file_key)
            if file_key == "PEP" and 'pcn_code' in data.columns:
                await check_column_duplicates(data, 'pcn_code', file_key)
            if file_key == "SRO" and 'nd_code' in data.columns:
                await check_column_duplicates(data, 'nd_code', file_key)
            if file_key == "SUPPORT" and 'pt_codeext' in data.columns:
                await check_column_duplicates(data, 'pt_codeext', file_key)
            if file_key == "SUPPORT" and 'pcn_id' in data.columns:
                await check_column_duplicates(data, 'pcn_id', file_key)
            if file_key == "ZNRO" and 'zn_code' in data.columns:
                await check_column_duplicates(data, 'zn_code', file_key)
            if file_key == "ZPA" and 'pcn_code' in data.columns:
                await check_column_duplicates(data, 'pcn_code', file_key)
            if file_key == "ZPBO" and 'pcn_code' in data.columns:
                await check_column_duplicates(data, 'pcn_code', file_key)
            if file_key == "ZSRO" and 'zs_code' in data.columns:
                await check_column_duplicates(data, 'zs_code', file_key)
    except Exception as e:
        print(f"Erreur lors de la vérification des doublons : {e}")

async def verify_cable_direction(cb_di_gdf, nro_gdf, sro_gdf, pa_gdf, pb_gdf, adresse_gdf):
    def get_zone_type(geom, zone_gdfs):
        for zone_type, zone_gdf in zone_gdfs.items():
            if zone_gdf.geometry.contains(geom).any():
                return zone_type
        return None

    zone_gdfs = {
        'NRO': nro_gdf,
        'SRO': sro_gdf,
        'PA': pa_gdf,
        'PB': pb_gdf,
        'ADRESSE': adresse_gdf
    }

    incorrect_direction_cables = []

    for idx, row in cb_di_gdf.iterrows():
        geom = row['geometry']
        if not isinstance(geom, LineString):
            continue

        source = Point(geom.coords[0])
        destination = Point(geom.coords[-1])

        source_zone_type = get_zone_type(source, zone_gdfs)
        destination_zone_type = get_zone_type(destination, zone_gdfs)

        if source_zone_type and destination_zone_type:
            if source_zone_type == destination_zone_type:
                    continue
            elif source_zone_type == 'NRO' and destination_zone_type == 'SRO':
                continue
            elif source_zone_type == 'SRO' and destination_zone_type == 'PA':
                continue
            elif source_zone_type == 'PA' and destination_zone_type == 'PB':
                continue
            elif source_zone_type == 'PB' and destination_zone_type == 'ADRESSE':
                continue
            else:
                incorrect_direction_cables.append(row['cl_codeext'])
        else:
            incorrect_direction_cables.append(row['cl_codeext'])

    if incorrect_direction_cables:
        print("Les cables suivants ont un sens incorrecte :")
        for cable in incorrect_direction_cables:
            print(f"- {cable}")

    return incorrect_direction_cables

#Table attributaire du NRO

async def verify_nd_code(gdf, table_type):
    missing_nd_code = gdf['nd_code'].isna() | (gdf['nd_code'] == '')
    
    if missing_nd_code.any():
        print(f"La colonne nd_code n'est pas remplie dans la table attributaire de {table_type}")
        return True
    else:
        return False

async def verify_nd_r3_code(nro_gdf):
    missing_nd_r3_code = nro_gdf['nd_r3_code'].isna() | (nro_gdf['nd_r3_code'] == '')
    
    if missing_nd_r3_code.any():
        print("La colonne nd_r3_code n'est pas remplie dans la table attributaire de NRO")
        return True
    else:
        return False

#Table attributaire deu ZNRO

async def verify_zn_code(znro_gdf):
    missing_nd_code = znro_gdf['zn_code'].isna() | (znro_gdf['zn_code'] == '')
    
    if missing_nd_code.any():
        print("La colonne zn_code n'est pas remplie dans la table attributaire de ZNRO")
        return True
    else:
        return False

async def verify_zn_nd_code(znro_gdf, nro_gdf):
    missing_mask = znro_gdf['zn_nd_code'].isna() | (znro_gdf['zn_nd_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zn_nd_code' contient des valeurs manquantes dans la table ZNRO")

    valid_codes = set(nro_gdf['nd_code'].dropna().unique())

    mismatch_mask = (~znro_gdf['zn_nd_code'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = znro_gdf.loc[mismatch_mask, 'zn_nd_code'].tolist()

    if invalid_codes:
        print("Les ZNRO suivants ont un 'zn_nd_code' non trouvé dans la table NRO :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zn_r1_code(znro_gdf):
    missing_zn_r1_code = znro_gdf['zn_r1_code'].isna() | (znro_gdf['zn_r1_code'] == '')
    
    if missing_zn_r1_code.any():
        print("La colonne zn_r1_code n'est pas remplie dans la table attributaire de ZNRO")
        return True
    else:
        return False

async def verify_zn_r2_code(znro_gdf):
    missing_zn_r2_code = znro_gdf['zn_r2_code'].isna() | (znro_gdf['zn_r2_code'] == '')
    
    if missing_zn_r2_code.any():
        print("La colonne zn_r2_code n'est pas remplie dans la table attributaire de ZNRO")
        return True
    else:
        return False

async def verify_zn_r3_code(znro_gdf, nro_gdf):
    missing_mask = znro_gdf['zn_r3_code'].isna() | (znro_gdf['zn_r3_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zn_r3_code' contient des valeurs manquantes dans la table ZNRO")

    valid_codes = set(nro_gdf['nd_r3_code'].dropna().unique())

    mismatch_mask = (~znro_gdf['zn_r3_code'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = znro_gdf.loc[mismatch_mask, 'zn_r3_code'].tolist()

    if invalid_codes:
        print("Les ZNRO suivants ont un 'zn_r3_code' non trouvé dans la table NRO :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zn_nroref(znro_gdf):
    if 'zn_nroref' not in znro_gdf.columns:
        raise KeyError("Colonne 'zn_nroref' absente de la table ZNRO")

    missing_mask = znro_gdf['zn_nroref'].isna() | (znro_gdf['zn_nroref'] == '')
    if missing_mask.any():
        print("La colonne 'zn_nroref' contient des valeurs manquantes ou vides dans ZNRO")

    pattern = re.compile(r'^\d{5}/NRO/[A-Z]{3}$')
    bad_format_mask = (~znro_gdf['zn_nroref'].str.match(pattern, na=False)) & (~missing_mask)  
    invalid = [(idx, val) 
               for idx, val in zip(znro_gdf.index[bad_format_mask], znro_gdf.loc[bad_format_mask, 'zn_nroref'])]
    if invalid:
        print("La ZNRO a un 'zn_nroref' mal formé (attendu '12345/NRO/ABC') :")
    return invalid

#Table attributaire du SRO
    
async def verify_nd_r4_code(sro_gdf):
    missing_nd_r4_code = sro_gdf['nd_r4_code'].isna() | (sro_gdf['nd_r4_code'] == '')
    
    if missing_nd_r4_code.any():
        print("La colonne nd_r4_code n'est pas remplie dans la table attributaire de SRO")
        return True
    else:
        return False

async def verify_pcn_cb_ent_sro(sro_gdf, adresse_gdf):
    invalid_pcn_cb_ent = []
    
    total_pcn_ftth = adresse_gdf['pcn_ftth'].sum()
    calculated_value = total_pcn_ftth / 6
    
    possible_values = [36, 72, 144]
    expected_pcn_cb_ent = min([val for val in possible_values if val >= calculated_value], default=None)
    
    if expected_pcn_cb_ent is None:
        print("Aucune valeur valide trouvée pour pcn_cb_ent.")
        return True
    
    for i, row in sro_gdf.iterrows():
        pcn_cb_ent = row['pcn_cb_ent']
        nd_code = row['nd_code']
        
        if pd.isna(pcn_cb_ent) or pcn_cb_ent == 0:
            print(f"La colonne pcn_cb_ent n'est pas remplie pour le SRO avec nd_code {nd_code}.")
            invalid_pcn_cb_ent.append(nd_code)
        elif pcn_cb_ent != expected_pcn_cb_ent:
            invalid_pcn_cb_ent.append(nd_code)
            print(f"SRO {nd_code} has pcn_cb_ent {pcn_cb_ent} which does not match the expected value {expected_pcn_cb_ent}.")
    
    return invalid_pcn_cb_ent

#Table attributaire du ZSRO

async def verify_zs_code(zsro_gdf):
    missing_zs_code = zsro_gdf['zs_code'].isna() | (zsro_gdf['zs_code'] == '')
    
    if missing_zs_code.any():
        print("La colonne zs_code n'est pas remplie dans la table attributaire de ZSRO")
        return True
    else:
        return False

async def verify_zs_nd_code(zsro_gdf, sro_gdf):
    missing_mask = zsro_gdf['zs_nd_code'].isna() | (zsro_gdf['zs_nd_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zs_nd_code' est vide dans la table ZSRO")

    valid_codes = set(sro_gdf['nd_code'].dropna().unique())

    mismatch_mask = (~zsro_gdf['zs_nd_code'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = zsro_gdf.loc[mismatch_mask, 'zs_nd_code'].tolist()

    if invalid_codes:
        print("La ZSRO a un 'zn_r3_code' non compatible avec 'nd_code' de la table SRO :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zs_zn_code(zsro_gdf, znro_gdf):
    missing_mask = zsro_gdf['zs_zn_code'].isna() | (zsro_gdf['zs_zn_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zs_zn_code' est vide dans la table ZSRO")

    valid_codes = set(znro_gdf['zn_code'].dropna().unique())

    mismatch_mask = (~zsro_gdf['zs_zn_code'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = zsro_gdf.loc[mismatch_mask, 'zs_zn_code'].tolist()

    if invalid_codes:
        print("La ZSRO a un 'zs_zn_code' non compatible avec 'zn_code' de la table ZNRO :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zs_r1_code(zsro_gdf, znro_gdf):
    missing_mask = zsro_gdf['zs_r1_code'].isna() | (zsro_gdf['zs_r1_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zs_r1_code' est vide dans la table ZSRO")

    valid_codes = set(znro_gdf['zn_r1_code'].dropna().unique())

    mismatch_mask = (~zsro_gdf['zs_r1_code'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = zsro_gdf.loc[mismatch_mask, 'zs_r1_code'].tolist()

    if invalid_codes:
        print("La ZSRO a un 'zs_r1_code' non compatible avec 'zn_r1_code' de la table ZNRO :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zs_r2_code(zsro_gdf, znro_gdf):
    missing_mask = zsro_gdf['zs_r2_code'].isna() | (zsro_gdf['zs_r2_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zs_r2_code' est vide dans la table ZSRO")

    valid_codes = set(znro_gdf['zn_r2_code'].dropna().unique())

    mismatch_mask = (~zsro_gdf['zs_r2_code'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = zsro_gdf.loc[mismatch_mask, 'zs_r2_code'].tolist()

    if invalid_codes:
        print("La ZSRO a un 'zs_r2_code' non compatible avec 'zn_r2_code' de la table ZNRO :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zs_r3_code(zsro_gdf, znro_gdf):
    missing_mask = zsro_gdf['zs_r3_code'].isna() | (zsro_gdf['zs_r3_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zs_r3_code' est vide dans la table ZSRO")

    valid_codes = set(znro_gdf['zn_r3_code'].dropna().unique())

    mismatch_mask = (~zsro_gdf['zs_r3_code'].isin(valid_codes)) & (~missing_mask)  

    invalid_codes = zsro_gdf.loc[mismatch_mask, 'zs_r3_code'].tolist()

    if invalid_codes:
        print("La ZSRO a un 'zs_r3_code' non compatible avec 'zn_r3_code' de la table ZNRO :")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zs_r4_code(zsro_gdf, sro_gdf):
    missing_mask = zsro_gdf['zs_r4_code'].isna() | (zsro_gdf['zs_r4_code'] == '') 
    if missing_mask.any():
        print(" La colonne 'zs_r4_code' est vide dans la table ZSRO")
    valid_codes = set(sro_gdf['nd_r4_code'].dropna().unique())
    mismatch_mask = (~zsro_gdf['zs_r4_code'].isin(valid_codes)) & (~missing_mask)  
    invalid_codes = zsro_gdf.loc[mismatch_mask, 'zs_r4_code'].tolist()

    if invalid_codes:
        print("La ZSRO a un 'zs_r4_code' non compatible avec 'zn_r4_code' de la table SRO:")
        for code in invalid_codes:
            print(f"  - {code}")

    return invalid_codes

async def verify_zs_refpm(zsro_gdf):
    missing_zs_refpm = zsro_gdf['zs_refpm'].isna() | (zsro_gdf['zs_refpm'] == '')
    
    if missing_zs_refpm.any():
        print("La colonne 'zs_refpm' n'est pas remplie dans la table attributaire de ZSRO")
        return True
    else:
        return False

async def verify_zs_capamax(zsro_gdf):
    invalid_zs_capamax = []
    valid_values = [576, 600, 720, 800, 864]
    
    for i, row in zsro_gdf.iterrows():
        zs_capamax = row['zs_capamax']
        zs_r4_code = row['zs_r4_code']
        
        if pd.isna(zs_capamax):
            print(f"ZSRO has zs_capamax which is missing.")
            invalid_zs_capamax.append(zs_r4_code)
        elif zs_capamax not in valid_values:
            print(f"ZSRO {zs_r4_code} has zs_capamax {zs_capamax} which is not among the valid values {valid_values}.")
            invalid_zs_capamax.append(zs_r4_code)
    
    return invalid_zs_capamax

async def verify_pcn_ftth(zpa_gdf, pb_gdf, zpbo_gdf, zsro_gdf, adresse_gdf):
    async def check_table(gdf, table_name):
        missing_pcn_ftth = gdf['pcn_ftth'].isna() | (gdf['pcn_ftth'] == '')
        
        if missing_pcn_ftth.any():
            print(f"La colonne pcn_ftth contient une/des valeurs manquantes dans la table {table_name}.")
            return False
        
        invalid_pcn_ftth = []
        for i, row in gdf.iterrows():
            geometry = row['geometry']
            za_pcn_ftth = row['pcn_ftth']
            
            adresse_within = adresse_gdf[adresse_gdf.within(geometry)]
            aggregate_pcn_ftth = adresse_within['pcn_ftth'].sum()
            
            if za_pcn_ftth != aggregate_pcn_ftth:
                invalid_pcn_ftth.append(row['pcn_code'])
                print(f"{table_name} {row['pcn_code']} a pcn_ftth {za_pcn_ftth} qui ne correspond pas au nombre d'EL: {aggregate_pcn_ftth}.")
        
        return invalid_pcn_ftth

    # Vérification pour chaque table
    invalid_zpa = check_table(zpa_gdf, 'ZPA')
    invalid_pb = check_table(pb_gdf, 'PB')
    invalid_zpbo = check_table(zpbo_gdf, 'ZPBO')
    invalid_zsro = check_table(zsro_gdf, 'ZSRO')

    return {
        'invalid_zpa': invalid_zpa,
        'invalid_pb': invalid_pb,
        'invalid_zpbo': invalid_zpbo,
        'invalid_zsro': invalid_zsro
    }

async def verify_pcn_ftte_zsro(zsro_gdf, adresse_gdf):
    missing_pcn_ftte = zsro_gdf['pcn_ftte'].isna() | (zsro_gdf['pcn_ftte'] == '')
    
    if missing_pcn_ftte.any():
        print("La colonne pcn_ftte n'est pas remplie dans la table attributaire de ZSRO")
        return False
    
    invalid_pcn_ftte = []
    for i, row in zsro_gdf.iterrows():
        zs_geometry = row['geometry']
        zs_pcn_ftte = row['pcn_ftte']
        
        adresse_within_zsro = adresse_gdf[adresse_gdf.within(zs_geometry)]
        aggregate_pcn_ftte = adresse_within_zsro['pcn_ftte'].sum()
        
        if zs_pcn_ftte != aggregate_pcn_ftte:
            invalid_pcn_ftte.append(row['zs_code'])
            print(f"ZSRO {row['zs_code']} a pcn_ftth {zs_pcn_ftte} qui ne correspond pas à la valeur correcte: {aggregate_pcn_ftte}.")
    
    return invalid_pcn_ftte

async def verify_pcn_umtot_zsro(zsro_gdf, pb_gdf):
    missing_pcn_umtot = zsro_gdf['pcn_umtot'].isna() | (zsro_gdf['pcn_umtot'] == '')
    
    if missing_pcn_umtot.any():
        print("La colonne pcn_umtot n'est pas remplie dans la table attributaire de ZSRO")
        return False
    
    invalid_pcn_umtot = []
    for i, row in zsro_gdf.iterrows():
        zs_geometry = row['geometry']
        zs_pcn_umtot = row['pcn_umtot']
        
        pb_within_zsro = pb_gdf[pb_gdf.within(zs_geometry)]
        correct_pcn_umtot = pb_within_zsro['pcn_umftth'].sum()
        
        if zs_pcn_umtot != correct_pcn_umtot:
            invalid_pcn_umtot.append(row['zs_code'])
            print(f"ZSRO {row['zs_code']} a pcn_ftth {zs_pcn_umtot} qui ne correspond pas à la valeur correcte: {correct_pcn_umtot}.")
    
    return invalid_pcn_umtot
