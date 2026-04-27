from models.config import PipelineConfig

def load_config(config_dict: dict) -> PipelineConfig:
    return PipelineConfig(**config_dict)
