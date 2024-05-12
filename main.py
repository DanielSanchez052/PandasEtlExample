import os
import json
from pipeline import Step, Pipeline, Transform, Load
from importlib.machinery import SourceFileLoader
import time
from settings import Settings

start = time.time()

if (__name__ == "__main__"):
    settings = Settings()
    tasks_file = open(settings.TASKS_FILE)
    tasks_data = json.load(tasks_file)
    extract_provider = None
    steps = []
    load_provider = None

    for task in tasks_data:
        print(f"Executing {task['name']} task")
        # get load provider
        extract_provider_module = SourceFileLoader("extract_provider", task["extract_provider"]["script"]).load_module()
        extract_provider = Step(getattr(extract_provider_module, task["extract_provider"]["method"]), **task["extract_provider"]["args"])

        # get steps providers
        for step in task["steps"]:
            step_provider_module = SourceFileLoader(step["name"], step["script"]).load_module()
            step_provider = getattr(step_provider_module, step["method"])

            if step["type"] == "Transform":
                step_provider = Transform(step_provider, **step["args"])
            elif step["type"] == "Step":
                step_provider = Step(step_provider, **step["args"])
            elif step["type"] == "Load":
                step_provider = Load(step_provider, **step["args"])

            steps.append(step_provider)

        # # get load provider
        load_provider_module = SourceFileLoader("load_provider", task["load_provider"]["script"]).load_module()
        load_provider = Load(getattr(load_provider_module, task["load_provider"]["method"]), **task["load_provider"]["args"])

        print(f"processesing {settings.PROCESS_DIR} ")
        pipeline = Pipeline(
            source=settings.PROCESS_DIR,
            extract=extract_provider,
            steps=steps,
            load=load_provider
        )
        pipeline.run()

        print('It took', time.time() - start, 'seconds.')
