from mangum import Mangum
from main import app

# AWS Lambda handler for the FastAPI app
handler = Mangum(app)
