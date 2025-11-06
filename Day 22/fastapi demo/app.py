from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import joblib
import numpy as np

app = FastAPI()

# Load model
model = joblib.load("cal_housing_model.pkl")

templates = Jinja2Templates(directory="template")


@app.get("/", response_class=HTMLResponse)
async def form_get(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/predict")
async def predict(
        request: Request,
        MedInc: float = Form(...),
        HouseAge: float = Form(...),
        AveRooms: float = Form(...),
        AveBedrms: float = Form(...),
        Population: float = Form(...),
        AveOccup: float = Form(...),
        Latitude: float = Form(...),
        Longitude: float = Form(...)
):
    input_data = np.array([[MedInc, HouseAge, AveRooms, AveBedrms,
                            Population, AveOccup, Latitude, Longitude]])
    price = model.predict(input_data)[0]
    price = round(price, 3)  # in $100,000 units

    return templates.TemplateResponse("result.html", {
        "request": request,
        "price": price,
        "MedInc": MedInc,
        "HouseAge": HouseAge,
        "AveRooms": AveRooms,
        "AveBedrms": AveBedrms,
        "Population": Population,
        "AveOccup": AveOccup,
        "Latitude": Latitude,
        "Longitude": Longitude
    })


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
