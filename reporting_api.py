from flask import Flask, request
from reportingsoftware import main

app = Flask(__name__)

# Trigger reporting making

@app.route('/report', methods=['GET'])
def get_report():
    try:  
        main()
        return {"message": "Report created and uploaded to Azure Blob Storage"}
    except:
        return {"error": "no data"}

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)