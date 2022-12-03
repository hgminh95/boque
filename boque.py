import click
import subprocess
import os
import json
import zmq
import time


class Task():
    def __init__(self, name, cmd):
        self.name = name
        self.cmd = cmd
        self.p = None

    @staticmethod
    def from_json(data):
        return Task(name=data["name"], cmd=data["cmd"])


class ShellExecutor():
    def __init__(self, log_folder):
        self._log_folder = log_folder

    def execute(self, task):
        if task.p is None:
            # TODO: is there a leak here?
            output = open(f"{self._log_folder}/{task.name}", "w")
            task.p = subprocess.Popen(
                task.cmd, shell=True, stdout=output, stderr=subprocess.STDOUT,
                executable='/bin/bash',
            )
        else:
            print(f"[WARNING] Task {task.name} is executed twice...")

    def print_stats(self):
        pass

class Scheduler():
    def __init__(self, executor, num_jobs):
        self._executor = executor
        self._num_jobs = num_jobs

        self._pending_tasks = []
        self._running_tasks = []
        self._task_names = set()

        self._pending_task_cnt = 0
        self._finished_task_cnt = 0
        self._running_task_cnt = 0

    def schedule(self, task):
        if task.name in self._task_names:
            return f"{task.name} already exists"

        print(f"Schedule new task {task.name}")
        self._task_names.add(task.name)
        self._pending_tasks.append(task)
        return "OK"

    def try_execute_once(self):
        # Go through all pending tasks to see if anything is done yet
        for idx, task in enumerate(self._running_tasks):
            if task.p.poll() is not None:
                print(f"Task {task.name} is finished with return code: {task.p.returncode}")
                del self._running_tasks[idx]

                self._finished_task_cnt += 1

                # We can cleanup other running tasks in the next run
                break

        # Try to find and execute another task
        if self._num_jobs <= len(self._running_tasks):
            return

        if len(self._pending_tasks) == 0:
            return

        task = self._pending_tasks.pop()
        print(f"Execute task {task.name}")
        self._executor.execute(task)
        self._running_tasks.append(task)

    def print_stats(self):
        print(f"Scheduler Stats: " \
            f"running: {len(self._running_tasks)} - " \
            f"pending: {len(self._pending_tasks)} - " \
            f"finished: {self._finished_task_cnt}")


@click.command()
@click.option("--port", default=5555, help="Port to bind to")
@click.option("--num_jobs", default=4, help="Max. number of task it should run at a time")
@click.option("--log_folder", default="./logs", help="Path to the logs folder")
def main(port, num_jobs, log_folder):
    executor = ShellExecutor(log_folder=log_folder)
    scheduler = Scheduler(executor, num_jobs=num_jobs)

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://127.0.0.1:{port}")

    print(f"Starting server at tcp://127.0.0.1:{port}")

    last_stats_tm = time.time() - 60

    while True:
        event = socket.poll(timeout=50)
        if event != 0:
            try:
                print(f"Receive new event from ZMQ socket: {event}")
                msg = socket.recv()
                task = Task.from_json(json.loads(msg.decode("utf-8")))

                socket.send_string(scheduler.schedule(task))
            except Exception as e:
                import traceback
                print(f"Exception: {e}")
                traceback.print_exc(e)

        scheduler.try_execute_once()

        if time.time() - last_stats_tm > 60:
            scheduler.print_stats()
            last_stats_tm = time.time()


if __name__ == "__main__":
    main()
