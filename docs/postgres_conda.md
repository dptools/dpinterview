Source:
[https://gist.github.com/gwangjinkim/f13bf596fefa7db7d31c22efd1627c7a](https://gist.github.com/gwangjinkim/f13bf596fefa7db7d31c22efd1627c7a)

Sure, I can help you with that. Here is the reformatted version of your gist in proper Markdown:

# How to install and start postgresql locally using conda

This gist I write, because I couldn't find step by step instructions
how to install and start postgresql locally (using conda within a conda environment - with the result
that you can run it without sudo/admin rights on your machine!)
and not globally in the operating system (which requires sudo/admin rights on that machine).

I hope, this will help especially people new to postgresql (and those who don't have sudo/admin rights on a specific machine but want
to run postgresql there)!

## Create conda environment

```bash
conda create --name myenv

# enter the environment
conda activate myenv
```

## Install postgresql via conda

```bash
conda install -y -c conda-forge postgresql
```

## Create a base database locally

```bash
initdb -D mylocal_db
```

## Start the server modus/instance of postgres

```bash
pg_ctl -D mylocal_db -l logfile start

# waiting for server to start.... done
# server started
```

### If you want to use a different port edit the following line in `mylocal_db/postgresql.conf`

```bash
# port = 5432
```

to

```bash
port = 58999
```


Now the server is up.

## Create a non-superuser (more safety!)

```bash
createuser --encrypted --pwprompt mynonsuperuser
# asks for name and password
```

If you used a different port
```bash
createuser --encrypted --pwprompt -h localhost -p 58999 mynonsuperuser
```

## Using this super user, create inner database inside the base database

```bash
createdb --owner=mynonsuperuser myinner_db
```

If you used a different port
```bash
createdb -h localhost -p 58999 --owner=mynonsuperuser myinner_db 
```

At this point, if you run some program,
you connect your program with this inner database
e.g. Django.

## Connect to the inner database via. psql

```bash
psql -d myinner_db -U mynonsuperuser
```

To use more relaxed listen address

1) Edit the following line in `postgresql.conf`

```bash
#listen_addresses = 'localhost'
```

to

```bash
listen_addresses = '*'
```

2) Add the following line in `pg_hba.conf` under `# IPv4 local connections:`

```bash
# IPv4 local connections: 
host    all             all             0.0.0.0/0               trust
```

## In this point, if you run some program, you connect your program with this inner database e.g. Django

In django (Python) e.g. you open with your favorite editor:

```bash
nano <mysite>/settings.py # or instead of nano your favorite editor!
```

And edit the `DATABASES` section as follows:

```python
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'myinner_db',
        'USER': 'mynonsuperuser',
        'PASSWORD': '<mynonsuperuserpassword>',
        'HOST': 'localhost',
        'PORT': '',
    }
}
```

And it is available for django, so that you can do the following steps.

## Do with the connected program these further steps

First install psycopg2, because django requires this for handling postgresql.

```bash
conda install -c anaconda psycopg2
```

Then now you can do:

```bash
python manage.py migrate
# to fully integreate the postgresql into your django website

# and to be able to use the database, you also need to create a superuser
python manage.py createsuperuser --username name 
```

## Stop running the postgres instance under ubuntu

Monitor whether a postgres instance/server is running or not.

```bash
ps aux | grep postgres
```

If no instance is running, you will see only one line as the answer to your query - which is from your grep search!
Ending with: `grep --color=auto postgres`
Ignore this line!
If an instance of postgresql server is running, then several processes are runnng.
You can kill the server by the first number of the leading line!

```bash
kill <number>
```

# e.g. the output of `ps aux | grep postgres` was

```bash
username  2673  0.0  0.0  14760   512 pts/11   S+   07:34   0:00 grep --color=auto postgres
username 30550  0.0  0.0 179144 18996 ?        S    Jun13   0:01 /home/username/miniconda3/envs/django/bin/postgres -D mylocal_db
username 30552  0.0  0.0 179276  4756 ?        Ss   Jun13   0:00 postgres: checkpointer process   
username 30553  0.0  0.0 179144  5216 ?        Ss   Jun13   0:01 postgres: writer process   
username 30554  0.0  0.0 179144  8464 ?        Ss   Jun13   0:01 postgres: wal writer process   
username 30555  0.0  0.0 179700  5792 ?        Ss   Jun13   0:01 postgres: autovacuum launcher process   
username 30556  0.0  0.0 34228    ?        Ss   Jun13   ?    postgres: stats collector process  
```

then # `2673` is just the `grep --color=auto postgres` so ignore
the line ending with `postgres -D /path/to/mylocal_db` is the leading line!
take first number occuring in this line (PID - process ID number) which is `30550`, therefore kill it by:

```bash
kill `30550`
```

## Run postgres as a non-server in the background

To run postgres as a non-server in the background, you can use the following command in your terminal:

```bash
postgres -D db_djangogirls & # runs postgres
# press RET (return) to send it to background!
```

You can stop and switch to server mode by following the instructions in [this article].

## Stop non-server or server modus/instance of postgres

To stop non-server or server modus/instance of postgres, you can use the following commands in your terminal:

```bash
ps aux | grep postgres # see detailed instructions for finding the correct <process ID> 
# under 'stop running postgres instance under ubuntu'! And then do:
kill <process ID> # to stop postgres
```

Have fun with your completely locally running - more safe - postgresql!!!
