from flask import Flask, request, jsonify, render_template
from werkzeug.utils import secure_filename
import os
import pandas as pd
from sqlalchemy import create_engine
from breached_processor_input import parse_sample_file, store_data_to_db  # Import functions from breach_processor.py


# Flask app initialization
app = Flask(__name__)


# Configuration
UPLOAD_FOLDER = './uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# PostgreSQL connection
DATABASE_URL = "postgresql://breachdb_owner:H6csGDI9Wkqw@ep-plain-haze-a8r6y8j7.eastus2.azure.neon.tech/breachdb"
engine = create_engine(DATABASE_URL)

@app.route('/')
def home():
    return """
    <h1>Welcome to the Breach Data Query API</h1>
    <p>Use the forms below to query data or upload a file:</p>

    <h3>Upload Sample File for Parsing</h3>
    <form action="/upload" method="post" enctype="multipart/form-data">
        Select file: <input type="file" name="file" required>
        <button type="submit">Upload and Parse</button>
    </form>

    <h3>Query by Domain</h3>
    <form action="/query/domain" method="get">
        Domain: <input type="text" name="domain" required>
        <button type="submit">Submit</button>
    </form>

    <h3>Query by Application</h3>
    <form action="/query/application" method="get">
        Application: <input type="text" name="application" required>
        <button type="submit">Submit</button>
    </form>

    <h3>Query by Port</h3>
    <form action="/query/port" method="get">
        Port: <input type="number" name="port" required>
        <button type="submit">Submit</button>
    </form>

    <h3>Query by Path</h3>
    <form action="/query/path" method="get">
        Path: <input type="text" name="path" required>
        <button type="submit">Submit</button>
    </form>

    <h3>Query by Tags</h3>
    <form action="/query/tags" method="get">
        Tag: <input type="text" name="tag" required>
        Exclude: <input type="checkbox" name="exclude" value="true">
        <button type="submit">Submit</button>
    </form>

    <h3>Query by Routable IPs</h3>
    <form action="/query/routable" method="get">
        Routable:
        <select name="routable">
            <option value="true">True</option>
            <option value="false">False</option>
        </select>
        <button type="submit">Submit</button>
    </form>
    """
# API to query by domain
@app.route('/query/domain', methods=['GET'])
def query_by_domain():
    domain = request.args.get('domain')
    if not domain:
        return jsonify({"error": "Domain is required"}), 400

    query = f"SELECT * FROM breach_data WHERE domain = '{domain}'"
    try:
        results = pd.read_sql_query(query, engine)
        return results.to_json(orient='records')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# API to query by application
@app.route('/query/application', methods=['GET'])
def query_by_application():
    application = request.args.get('application')
    if not application:
        return jsonify({"error": "Application is required"}), 400

    query = f"SELECT * FROM breach_data WHERE application = '{application}'"
    try:
        results = pd.read_sql_query(query, engine)
        return results.to_json(orient='records')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400

    # Save the uploaded file
    filename = secure_filename(file.filename)
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(file_path)

    try:
        # Parse the uploaded file
        parsed_df = parse_sample_file(file_path)
        store_data_to_db(parsed_df)
        return jsonify({"message": "File parsed and data stored successfully"}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to process file: {str(e)}"}), 500

# API to query by port
@app.route('/query/port', methods=['GET'])
def query_by_port():
    port = request.args.get('port')
    if not port:
        return jsonify({"error": "Port is required"}), 400

    query = f"SELECT * FROM breach_data WHERE port = {port}"
    try:
        results = pd.read_sql_query(query, engine)
        return results.to_json(orient='records')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# API to query by path
@app.route('/query/path', methods=['GET'])
def query_by_path():
    path = request.args.get('path')
    if not path:
        return jsonify({"error": "Path is required"}), 400

    query = f"SELECT * FROM breach_data WHERE path LIKE '%{path}%'"
    try:
        results = pd.read_sql_query(query, engine)
        return results.to_json(orient='records')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# API to query by tags
@app.route('/query/tags', methods=['GET'])
def query_by_tags():
    tag = request.args.get('tag')
    exclude = request.args.get('exclude', 'false').lower() == 'true'
    if not tag:
        return jsonify({"error": "Tag is required"}), 400

    if exclude:
        query = f"SELECT * FROM breach_data WHERE NOT tags::jsonb @> '[\"{tag}\"]'"
    else:
        query = f"SELECT * FROM breach_data WHERE tags::jsonb @> '[\"{tag}\"]'"
    try:
        results = pd.read_sql_query(query, engine)
        return results.to_json(orient='records')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# API to query routable IPs
@app.route('/query/routable', methods=['GET'])
def query_routable():
    routable = request.args.get('routable', 'true').lower() == 'true'

    query = f"SELECT * FROM breach_data WHERE routable = {routable}"
    try:
        results = pd.read_sql_query(query, engine)
        return results.to_json(orient='records')
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Start the Flask app
if __name__ == '__main__':
    app.run(debug=True)
