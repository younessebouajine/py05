from abc import ABC, abstractmethod
from typing import Any, Protocol, List, Union, Dict
from collections import deque, Counter


# =========================================================
# Protocol for stages (duck typing)
# =========================================================

class ProcessingStage(Protocol):
    def process(self, data: Any) -> Any:
        ...


# =========================================================
# Pipeline Base Class (ABC)
# =========================================================

class ProcessingPipeline(ABC):

    def __init__(self, pipeline_id: str) -> None:
        self.pipeline_id: str = pipeline_id
        self.stages: deque[ProcessingStage] = deque()
        self.stats: Counter = Counter()

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    def run_stages(self, data: Any) -> Any:
        """Run all stages sequentially"""
        for stage in self.stages:
            try:
                data = stage.process(data)
                self.stats["processed"] += 1
            except Exception as e:
                print(f"[ERROR] Stage failure: {e}")
                self.stats["errors"] += 1
                raise
        return data

    def get_stats(self) -> Dict[str, int]:
        return dict(self.stats)

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        pass


# =========================================================
# Processing Stages
# =========================================================

class InputStage:
    def process(self, data: Any) -> Any:
        print("Stage 1: Input validation")

        if data is None:
            raise ValueError("Invalid input")

        return data


class TransformStage:
    def process(self, data: Any) -> Any:
        print("Stage 2: Transforming data")

        if isinstance(data, dict):
            data["processed"] = True

        elif isinstance(data, list):
            data = [str(x) for x in data]

        elif isinstance(data, str):
            data = data.strip()

        return data


class OutputStage:
    def process(self, data: Any) -> Any:
        print("Stage 3: Formatting output")
        return f"OUTPUT => {data}"


# =========================================================
# Adapters (inherit from ProcessingPipeline)
# =========================================================

class JSONAdapter(ProcessingPipeline):

    def process(self, data: Any) -> Union[str, Any]:
        print(f"\n[JSON Pipeline: {self.pipeline_id}]")

        if not isinstance(data, dict):
            raise TypeError("JSONAdapter expects dict")

        return self.run_stages(data)


class CSVAdapter(ProcessingPipeline):

    def process(self, data: Any) -> Union[str, Any]:
        print(f"\n[CSV Pipeline: {self.pipeline_id}]")

        if isinstance(data, str):
            data = data.split(",")

        return self.run_stages(data)


class StreamAdapter(ProcessingPipeline):

    def process(self, data: Any) -> Union[str, Any]:
        print(f"\n[Stream Pipeline: {self.pipeline_id}]")

        if isinstance(data, list):
            data = {"stream_size": len(data), "data": data}

        return self.run_stages(data)


# =========================================================
# Nexus Manager
# =========================================================

class NexusManager:

    def __init__(self) -> None:
        self.pipelines: List[ProcessingPipeline] = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.pipelines.append(pipeline)

    def execute(self, data: Any) -> Any:
        """Run data through all pipelines sequentially (pipeline chaining)"""

        for pipeline in self.pipelines:
            try:
                data = pipeline.process(data)
            except Exception as e:
                print(f"[RECOVERY] Pipeline {pipeline.pipeline_id} failed: {e}")

        return data

    def show_stats(self) -> None:
        print("\n=== Pipeline Statistics ===")

        for pipeline in self.pipelines:
            print(pipeline.pipeline_id, pipeline.get_stats())


# =========================================================
# Demo (main)
# =========================================================

def main() -> None:

    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===")

    manager = NexusManager()

    # Create pipelines
    json_pipeline = JSONAdapter("JSON_01")
    csv_pipeline = CSVAdapter("CSV_01")
    stream_pipeline = StreamAdapter("STREAM_01")

    # Add stages to each pipeline
    for pipeline in [json_pipeline, csv_pipeline, stream_pipeline]:
        pipeline.add_stage(InputStage())
        pipeline.add_stage(TransformStage())
        pipeline.add_stage(OutputStage())

    # Register pipelines in manager
    manager.add_pipeline(json_pipeline)
    manager.add_pipeline(csv_pipeline)
    manager.add_pipeline(stream_pipeline)

    # Example data
    json_data = {"sensor": "temp", "value": 23.5}
    csv_data = "user,login,10:00"
    stream_data = [10, 20, 30, 40]

    print("\n--- JSON DATA ---")
    result = manager.execute(json_data)
    print("Final Result:", result)

    print("\n--- CSV DATA ---")
    result = manager.execute(csv_data)
    print("Final Result:", result)

    print("\n--- STREAM DATA ---")
    result = manager.execute(stream_data)
    print("Final Result:", result)

    manager.show_stats()


if __name__ == "__main__":
    main()