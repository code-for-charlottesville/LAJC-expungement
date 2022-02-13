# Expungement Classification Config Files

This is a directory for YAML configuration files for runs of the expungement classification pipeline. 
Besides `default.yaml`, any configuration YAML files in this directory will be ignored by git. 

To create a new configuration, copy `default.yaml` and rename it appropriately. Change `run_id` and any other
configuration values. You can then execute the run by passing the YAML file's path to `run.py`: 

```bash
python run.py --config configs/<my_config_file_name>.yaml
```

