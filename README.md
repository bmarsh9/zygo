Looker

### Get started
```commandline
docker-compose up -d postgres rabbitmq worker
export SQLALCHEMY_DATABASE_URI=postgresql://db1:db1@localhost/db1
python3 manage.py runserver -h 0.0.0.0 -p 8080 --debug

```
