from flask import Flask
from flask_cors import CORS
import threading
from delete_temp_files import delete_temp_files
from routes.upload import upload_blueprint

app = Flask(__name__)
CORS(app)

app.register_blueprint(upload_blueprint)

threading.Thread(target=delete_temp_files, daemon=True).start()

if __name__ == "__main__":
    app.run(debug=True)
