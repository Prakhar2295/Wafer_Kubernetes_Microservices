from flask import Flask,request,jsonify
import pandas as pd
import os
from prediction_validation_insertion import pred_validation
from predictFromModel import prediction

app = Flask(__name__)


@app.route('/predict',methods = ['POST'])
def predict():
    
    os.mkdir("raw_data",exist_ok=True)
    uploaded_file = request.files['file']
    
    if uploaded_file:
        
        file_path = os.path.join("raw_data",uploaded_file.filename)
        
        uploaded_file.save(file_path)
        path = "raw_data"
        pred_valid = pred_validation(path)
        pred_valid.prediction_validation()
        pred=prediction()
        result = pred.predictionfrommodel()
        
        return jsonify({'prediction': result.tolist()})
    
@app.route('/get_prediction',methods=['GET'])
def get_prediction():
    
    file_path = "default_file"
    if file_path is not None:
        pred_valid = pred_validation(file_path)
        pred_valid.prediction_validation()
        pred=prediction()
        result = pred.predictionfrommodel()
        
        return jsonify({'prediction': result})
        

        
if __name__=="__main__":
    app.run(host = '0.0.0.0',port = 6000)
        
        
    
    
    
   