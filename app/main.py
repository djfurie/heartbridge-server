from fastapi import FastAPI, WebSocket, HTTPException
from heartbridge import HeartBridgeServer, HeartBridgeConnection
from heartbridge.payload_types import HeartBridgeRegisterReturnPayload, HeartBridgeRegisterPayload, \
    HeartBridgeUpdatePayload, HeartBridgeSubscribePayload, HeartBridgePerformanceDetailsPayload, \
    HeartBridgePerformancesPayload, HeartBridgeDeleteReturnPayload, HeartBridgeDeletePayload
import logging

LOGGING_FORMAT = '%(asctime)s :: %(name)s (%(levelname)s) -- %(message)s'
logging.basicConfig(format=LOGGING_FORMAT, level=logging.DEBUG)

# High level class instantiations
app = FastAPI()
hbserver = HeartBridgeServer()


@app.websocket("/")
async def websocket_endpoint(websocket: WebSocket):
    conn = HeartBridgeConnection(websocket)
    await hbserver.add_connection(conn)


# @app.websocket("/subscribe/{performance_id}")
# async def websocket_subscribe_endpoint(websocket: WebSocket, performance_id: str):
#     conn = HeartBridgeConnection(websocket)
#     await hbserver.add_connection(conn)
#
#     p = HeartBridgeSubscribePayload(action="subscribe", performance_id=performance_id)
#     await hbserver.subscribe_handler(conn, p)


@app.post("/register", response_model=HeartBridgeRegisterReturnPayload)
async def rest_register_endpoint(payload: HeartBridgeRegisterPayload):
    ret = await hbserver.register_handler(payload)
    if "error" in ret:
        raise HTTPException(status_code=400, detail=ret)
    return ret


@app.post("/update", response_model=HeartBridgeRegisterReturnPayload)
async def rest_update_endpoint(payload: HeartBridgeUpdatePayload):
    ret = await hbserver.update_handler(payload)
    if "error" in ret:
        raise HTTPException(status_code=400, detail=ret)
    return ret


@app.post("/delete", response_model=HeartBridgeDeleteReturnPayload)
async def rest_delete_endpoint(payload: HeartBridgeDeletePayload):
    ret = await hbserver.delete_handler(payload)
    if "error" in ret:
        raise HTTPException(status_code=400, detail=ret)
    return ret


@app.get("/events/{performance_id}", response_model=HeartBridgePerformanceDetailsPayload)
async def rest_get_details(performance_id: str):
    ret = await hbserver.get_event_details(performance_id)
    if "error" in ret:
        raise HTTPException(status_code=400, detail=ret)
    return ret


@app.get("/events/", response_model=HeartBridgePerformancesPayload)
async def rest_get_performances():
    ret = await hbserver.get_events()
    if "error" in ret:
        raise HTTPException(status_code=400, detail=ret)
    return ret
