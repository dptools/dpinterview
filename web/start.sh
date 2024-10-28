cd /home/dm2637/dev/av-pipeline-v2/web
sudo /home/dm2637/miniforge3/envs/feat-extract/bin/gunicorn -w 10 --timeout 120 --bind 127.0.0.1:45000 wsgi:app
