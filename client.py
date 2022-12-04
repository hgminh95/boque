import click
import zmq
import json


@click.command()
@click.option("--addr", default="tcp://localhost:5555", help="Server to connect to")
@click.option("--name", help="Name of the task", required=True)
@click.option("--cmd", help="Cmd of the task", required=True)
@click.option("--resource", help="Resource required", required=False)
def main(addr, name, cmd, resource):
    context = zmq.Context()

    socket = context.socket(zmq.REQ)
    socket.connect(addr)

    print(f"Sending request to {addr}")

    data = {
        "name": name,
        "cmd": cmd,
    }
    if resource is not None:
        data["resource"] = resource

    socket.send_string(json.dumps(data))
    message = socket.recv()
    print(f"Receive: {message.decode('utf-8')}")

    
if __name__ == "__main__":
    main()
