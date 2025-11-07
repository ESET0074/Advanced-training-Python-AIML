from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import numpy as np
import pandas as pd
from tensorflow.keras.models import load_model
from sklearn.compose import make_column_transformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline
import joblib

# Initialize app
app = FastAPI()
templates = Jinja2Templates(directory="template")

# Load trained Keras model
model = load_model("hotel_cancellation_model.keras")

# Define preprocessing same as training
features_num = [
    "lead_time", "arrival_date_week_number", "arrival_date_day_of_month",
    "stays_in_weekend_nights", "stays_in_week_nights", "adults", "children",
    "babies", "is_repeated_guest", "previous_cancellations",
    "previous_bookings_not_canceled", "required_car_parking_spaces",
    "total_of_special_requests", "adr"
]

features_cat = [
    "hotel", "arrival_date_month", "meal", "market_segment",
    "distribution_channel", "reserved_room_type", "deposit_type", "customer_type"
]

transformer_num = make_pipeline(
    SimpleImputer(strategy="constant"),
    StandardScaler(),
)
transformer_cat = make_pipeline(
    SimpleImputer(strategy="constant", fill_value="NA"),
    OneHotEncoder(handle_unknown='ignore'),
)
preprocessor = make_column_transformer(
    (transformer_num, features_num),
    (transformer_cat, features_cat),
)

df = pd.read_csv("hotel.csv")
df['arrival_date_month'] = df['arrival_date_month'].map({
    'January':1, 'February':2, 'March':3, 'April':4, 'May':5, 'June':6,
    'July':7, 'August':8, 'September':9, 'October':10, 'November':11, 'December':12
})
X = df.copy()
y = X.pop('is_canceled')
preprocessor.fit(X)

@app.get("/", response_class=HTMLResponse)
async def form_page(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/predict", response_class=HTMLResponse)
async def predict(
    request: Request,
    hotel: str = Form(...),
    arrival_date_month: int = Form(...),
    lead_time: float = Form(...),
    arrival_date_week_number: int = Form(...),
    arrival_date_day_of_month: int = Form(...),
    stays_in_weekend_nights: int = Form(...),
    stays_in_week_nights: int = Form(...),
    adults: int = Form(...),
    children: float = Form(...),
    babies: int = Form(...),
    is_repeated_guest: int = Form(...),
    previous_cancellations: int = Form(...),
    previous_bookings_not_canceled: int = Form(...),
    required_car_parking_spaces: int = Form(...),
    total_of_special_requests: int = Form(...),
    adr: float = Form(...),
    meal: str = Form(...),
    market_segment: str = Form(...),
    distribution_channel: str = Form(...),
    reserved_room_type: str = Form(...),
    deposit_type: str = Form(...),
    customer_type: str = Form(...)
):
    # Prepare dataframe
    data = pd.DataFrame([{
        "hotel": hotel,
        "arrival_date_month": arrival_date_month,
        "lead_time": lead_time,
        "arrival_date_week_number": arrival_date_week_number,
        "arrival_date_day_of_month": arrival_date_day_of_month,
        "stays_in_weekend_nights": stays_in_weekend_nights,
        "stays_in_week_nights": stays_in_week_nights,
        "adults": adults,
        "children": children,
        "babies": babies,
        "is_repeated_guest": is_repeated_guest,
        "previous_cancellations": previous_cancellations,
        "previous_bookings_not_canceled": previous_bookings_not_canceled,
        "required_car_parking_spaces": required_car_parking_spaces,
        "total_of_special_requests": total_of_special_requests,
        "adr": adr,
        "meal": meal,
        "market_segment": market_segment,
        "distribution_channel": distribution_channel,
        "reserved_room_type": reserved_room_type,
        "deposit_type": deposit_type,
        "customer_type": customer_type,
    }])

    # Transform input
    transformed = preprocessor.transform(data)

    # Predict
    pred = model.predict(transformed)[0][0]
    prediction_label = "Will Cancel" if pred >= 0.5 else "Will Not Cancel"

    return templates.TemplateResponse("result.html", {
        "request": request,
        "prediction_label": prediction_label,
        "probability": round(float(pred), 3)
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
