import os
import uvicorn
from app import constants

if __name__ == "__main__":
    hostip = "127.0.0.1"
    # hostip = constants.SERVER_IP
    uvicorn.run("app.api:data_extraction", 
                # host=hostip, port=constants.SERVER_PORT, reload=True)
                host=hostip, port=8000, reload=True)
    