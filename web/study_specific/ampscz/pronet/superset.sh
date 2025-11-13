export SUPERSET_CONFIG_PATH=/home/dm2637/config/superset_config.py
export FLASK_APP=superset

/home/dm2637/miniforge3/envs/superset/bin/superset run -p 8088 --with-threads --reload --debugger
