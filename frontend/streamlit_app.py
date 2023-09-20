import streamlit as st
import requests
import os

def main():
    st.title("Wafer Fault Sensor Prediction App")
    
    st.markdown("""
    ## **Dataset Information : **            
    **This dataset is provided by the client side.
    This is a simple Frontend UI for showing prediction results.**""",True)
    html_temp = """
    <div style="background-color:tomato;padding:10px">
    <h2 style="color:white;text-align:center;">Wafer Fault Sensor Prediction App </h2>
    </div>
    """
    st.markdown(html_temp, unsafe_allow_html=True)

    if st.button("Custom File Predict"):
        uploaded_file = st.file_uploader("Upload a File")

        if uploaded_file is not None:
            original_file_name = uploaded_file.name
            st.write(f"Original File Name: {original_file_name}")


        if uploaded_file:

            #df =pd.read_csv(csv_file)

            files = {uploaded_file.name: uploaded_file.getvalue()}
            #st.dataframe(df)

            #data = df.to_json(orient="records")

            backend_servicename = os.environ.get('BACKEND_SERVICE_NAME')
            #backend_servicename = "http://127.0.0.1"
            response = requests.post(f"{backend_servicename}:6000/predict",files = files)

            prediction = response.json()["prediction"]
            st.write(f"prediction: {prediction}")
            
    st.markdown("OR")
    
    if st.button("Get Default File Prediction from backend"):
            backend_servicename = os.environ.get('BACKEND_SERVICE_NAME')
            #backend_servicename = "http://127.0.0.1:6000"
            response= requests.get(f"{backend_servicename}/get_prediction")
            #response = requests.get("http://127.0.0.1:6000/get_prediction")
            st.write(f"Backend Response: {response.text}")

        
            
    if st.button("About"):
        st.markdown("""**Built with ❤️ by Prakhar**""")
            
            
            
if __name__=="__main__":
    main()
        
        
        
        
        
        
        
