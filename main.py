from fastapi import FastAPI,Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import paramiko 
import json
import time
import sys
app = FastAPI()

class User(BaseModel):
    username: str
    password: str 


# class Request(BaseModel):
#     payload: str
#####  WORKER NAME to test
##### clrv0000202739.ic.ing.net
##### /opt/connect-common/bin/manageworker.sh start/stop

#change app.get to correct mthod

#unused
def run_command(cmds):
    for command in cmds:
        stdin, stdout, stderr = c.exec_command(command)
        output = stdout.read()
        err = stderr.read()

def ssh_conn(workerName):
    k = paramiko.RSAKey.from_private_key_file("id_{}.pem".format(workerName))
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(hostname = "{}.ic.ing.net".format(workerName), username = "kafka_npa", pkey=k) 
    return c


client = paramiko.SSHClient()
client.load_system_host_keys()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

@app.get("/")
async def root():
    return {"message":"index"}

@app.post("/api/generateKey/{workerName}")
async def generateKey(workerName: str):
    try:
        key = paramiko.RSAKey.generate(4096)
        print(key.get_base64())
        f = open("id_{}.pub".format(workerName), "w")
        f.write(key.get_base64())
        f.close()
        key.write_private_key(sys.stdout)
        key.write_private_key_file("id_{}".format(workerName))
        return JSONResponse(status_code=201, content=key.get_base64())
    except Exception as e:
        c.close
        return JSONResponse(status_code=400)


@app.get("/api/testKey/{workerName}")
async def testKey(workerName: str):
    try:
        c=ssh_conn(workerName)
        command="whoami"
        stdin, stdout, stderr = c.exec_command(command)
        output=stdout.read()
        err=stderr.read()
        c.close()
        if(output==b'kafka_npa\n'):
            return JSONResponse(status_code=200)
        else:
            return JSONResponse(status_code=400)
    except Exception as e:
        c.close()
        return JSONResponse(status_code=400)



#1 

@app.get("/api/{workerName}/{port}/connectors/{connectorName}/status/")
async def connectorStatus(workerName ,port, connectorName):
    try:
        c=ssh_conn(workerName)
        command = "curl -X GET localhost:{}/connectors/{}/status".format(port,connectorName)
        stdin, stdout, stderr = c.exec_command(command,3)
        output=stdout.read()
        err=stderr.read()
        res = json.loads(output.decode("utf8"))
        client.close()

        # Print output of command. Will wait for command to finish.
        return res
    except Exception as err:
        c.close()
        return JSONResponse(status_code=500)

@app.post("/api/{workerName}/{port}/connectorsStatus")
async def connectorStatus(workerName ,port,request:Request):
    try:
        payload = await request.json()
        cnStatus = {}
        c=ssh_conn(workerName)
        for connector in payload['connectors']:
            command = "curl -X GET localhost:{}/connectors/{}/status".format(port,connector)
            stdin, stdout, stderr = c.exec_command(command,3)
            output=stdout.read()
            err=stderr.read()
            res = json.loads(output.decode("utf8"))
            try:
                x = res['connector']['state']
                cnStatus[connector] = x
            except KeyError:
                cnStatus[connector]="NOT RUNNING"
                pass
       
        
        client.close()

        # Print output of command. Will wait for command to finish.
    
        return {"connectors":cnStatus}
    except Exception as err:
        c.close()
        return JSONResponse(status_code=500)


@app.get("/api/getConnectors/{workerName}/{port}")
def connectorsList(workerName,port):
    try:
        c=ssh_conn(workerName)
        command = "curl -X GET localhost:{}/connectors".format(port)
        stdin, stdout, stderr = c.exec_command(command,3)
        output=stdout.read()
        err=stderr.read()
        time.sleep(1)
        res = json.loads(output.decode("utf8"))
        client.close()

        # Print output of command. Will wait for command to finish.
        return {"connectors":res}
    except Exception as err:
        c.close()
        return JSONResponse(status_code=500)


#2 connector-plugins
@app.get("/api/{workerName}/{port}/connector-plugins")
async def connectorPlugins(workerName,port):
    try:
        c=ssh_conn(workerName)
        command = "curl -X GET localhost:{}/connector-plugins".format(port)
        stdin, stdout, stderr = c.exec_command(command,3)
        output=stdout.read()
        err=stderr.read()
        res = json.loads(output.decode("utf8"))

        client.close()

        # Print output of command. Will wait for command to finish.
        return res
    except Exception as err:
        client.close()
        return JSONResponse(status_code=500)


#3 post connectors
@app.post("/api/{workerName}/{port}/connectors")
async def postConnectors(workerName, port, request: Request):
    try:
        payload = await request.json()
        c=ssh_conn(workerName)
        command = "curl -X POST localhost:{}/connectors -H 'Content-Type: application/json' -d '{}'".format(port,json.dumps(payload))
        stdin, stdout, stderr = c.exec_command(command,3)

        output=stdout.read()
        err=stderr.read()
        res = json.loads(output.decode("utf8"))

        client.close()
        return JSONResponse(status_code=200)

    except Exception as err:
        client.close()
        return JSONResponse(status_code=500)


#4 delete connectors
@app.delete("/api/{workerName}/{port}/connectors/{connectorName}")
async def deleteConnectors(workerName, port, connectorName):
    try:
        c=ssh_conn(workerName)
        command = "curl -X DELETE localhost:{}/connectors/{}".format(port,connectorName)
        stdin, stdout, stderr = c.exec_command(command,3)
        output=stdout.read()
        err=stderr.read()
        res = json.loads(output.decode("utf8"))

        client.close()
        return JSONResponse(status_code = 200, content = res)

    except Exception as err:
        client.close()
        return JSONResponse(status_code=500)

#5 put connector plugins
@app.put("/api/{workerName}/{port}/connector-plugins/{connectorClass}/config/validate")
async def putConnectors(workerName, port, connectorClass, request: Request):
    try:
        payload = await request.json()
        c=ssh_conn(workerName)
        command = "curl -X PUT  localhost:{}/connector-plugins/{}/config/validate -H 'Content-Type: application/json' -d '{}'".format(port,connectorClass,json.dumps(payload))
        stdin, stdout, stderr = c.exec_command(command,3)
        output=stdout.read()
        err=stderr.read()
        res = json.loads(output.decode("utf8"))

        client.close()
        return JSONResponse(status_code = 200, content = res)

    except Exception as err:
        client.close()
        return JSONResponse(status_code=500)

#@app.post("/{worker_name}/{port}/connector-plugins")
@app.get("/api/connectorPlugins/{workerName}/{port}")
def connectorPlugins(workerName,port):
    try:
        c=ssh_conn(workerName)
        command = "curl -X GET localhost:{}/connector-plugins".format(port)
        stdin, stdout, stderr = c.exec_command(command,3)
        output=stdout.read()
        err=stderr.read()
        time.sleep(1)
        res = json.loads(output.decode("utf8"))

        client.close()

        # Print output of command. Will wait for command to finish.
        return {"connectors":res}
    except Exception as err:
        client.close()
        return JSONResponse(status_code=500)

@app.get("/api/startConnector/{workerName}/{port}")
#@app.post("/{worker_name}/{port}/start-connector")
async def startWorker(workerName,port,request: Request):
    try:
        #payload = await request.json()
       
        c=ssh_conn(workerName)
        command = "/opt/luca/connect_common/connect_common6.1test/bin/manageworker.sh start"
        stdin, stdout, stderr = c.exec_command(command,3)
        output=stdout.read()
        err=stderr.read()
        time.sleep(1)
        res = json.loads(output.decode("utf8"))

        client.close()

        # Print output of command. Will wait for command to finish.
        return payload
    except Exception as err:
        client.close()

@app.post("/api/{workerName}/{port}")
async def workerStatus(workerName,port,request:Request):
    try:
        status = ""
        payload = await request.json()
        c=ssh_conn(workerName)
        command = "{}/manageworker.sh status".format(payload["path"])
        print(command)
        stdin, stdout, stderr = c.exec_command(command,3)
        output=stdout.read()
        if 'Kafka Worker is running' in str(output):
            status="RUNNING"
        else:
            status="STOPPED"
        err=stderr.read()
        time.sleep(1)
        client.close()
        # Print output of command. Will wait for command to finish.
        return {"status":status}
    except Exception as err:
        client.close()
        