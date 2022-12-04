# boque

A small utility to queue tasks.

## Usage

Need Python 3.9

Run the server

```bash
$ python3 boque.py
```

To send the request to execute a new task, run below command

```bash
$ python3 client.py --cmd "echo 5000; sleep 50" --name test13
```

You can also specify the resource it requires

```bash
$ python3 client.py --cmd "CUDA_VISIBLE_DEVICES={cuda_device} echo 5000; sleep 50" --name test3 --resource cuda_device
```
