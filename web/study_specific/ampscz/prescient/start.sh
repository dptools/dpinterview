cd /var/lib/prescient/soft/av-pipeline/dpinterview/web
/var/lib/prescient/soft/av-pipeline/miniforge3/envs/jupyter/bin/gunicorn -w 10 --timeout 120 --bind 127.0.0.1:45000 wsgi:app
