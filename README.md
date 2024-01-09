# multinet-api

## Develop with Docker (recommended quickstart)
This is the simplest configuration for developers to start with.

### Initial Setup
1. Run `docker-compose run --rm django ./manage.py migrate`
2. Run `docker-compose run --rm django ./manage.py createsuperuser`
   and follow the prompts to create your own user. Make sure to supply an email address.
3. Run `docker-compose run --rm django ./manage.py createarangoreadonlyuser`
4. To hydrate the database with the demo data, run `docker-compose run --rm django ./manage.py setupdevenv <user_email from above step>`

### Run Application
1. Run `docker-compose up`
2. Access the site, starting at http://localhost:8000/admin/ with your email from above as the username.
3. When finished, use `Ctrl+C`

### Application Maintenance
Occasionally, new package dependencies or schema changes will necessitate
maintenance. To non-destructively update your development stack at any time:
1. Run `docker-compose pull`
2. Run `docker-compose build --pull --no-cache`
3. Run `docker-compose run --rm django ./manage.py migrate`
4. Run `docker-compose run --rm arangodb arangod --database.auto-upgrade`

## Develop Natively (advanced)
This configuration still uses Docker to run attached services in the background,
but allows developers to run Python code on their native system.

### Initial Setup
1. Run `docker-compose -f ./docker-compose.yml up -d`
2. Install Python 3.12
3. Install
   [`psycopg2` build prerequisites](https://www.psycopg.org/docs/install.html#build-prerequisites)
4. Create and activate a new Python virtualenv
5. Run `pip install -e .[dev]`
6. Run `source ./dev/export-env.sh`
7. Run `./manage.py migrate`
8. Run `./manage.py createsuperuser` and follow the prompts to create your own user. Make sure to supply an email address.
9. Run `./manage.py createarangoreadonlyuser`
10. To hydrate the database with the demo data, run `./manage.py setupdevenv <user_email from above step>`

### Run Application
1.  Ensure `docker-compose -f ./docker-compose.yml up -d` is still active
2. Run:
   1. `source ./dev/export-env.sh`
   2. `./manage.py runserver`
3. Run in a separate terminal:
   1. `source ./dev/export-env.sh`
   2. `celery --app multinet.celery worker --loglevel INFO --without-heartbeat`
4. When finished, run `docker-compose stop`

## Setup OAuth login
### API
1. Navigate to the http://localhost:8000/admin, logging in with your admin user if you're not already logged in
2. Under the `DJANGO OAUTH TOOLKIT` section click the `Add` in the `Applications` row. This will bring you to the creation dialog for an (oauth) application.
3. In the redirect uris field, enter `http://localhost:8080/` (**must** include trailing slash)
4. Select `Public` for the Client Type field
5. Select `Authorization Code` for the Authorization Grant Type field
6. Delete everything in the Client Secret field, leaving it blank
7. Enter "Multinet GUI" for the Name field
8. Copy the value in the Client ID field, but don't modify it
9. In the bottom right, click Save

### Client
1. If you haven't already, copy the `.env.default` file to `.env`
2. Ensure there's a line that reads `VUE_APP_OAUTH_API_ROOT=http://localhost:8000/oauth/`
3. Ensure there's a line that reads `VUE_APP_OAUTH_CLIENT_ID=<the client id you copied above>`
4. Save the file
5. Restart your local dev server

After these steps, you should be able to login to the API from the client.

## Setup Google OAuth Provider
### API
1. Create a google oauth token through the google cloud dashboard in `APIs and Services` > `Credentials`.
2. Navigate to the http://<server address>/admin, logging in with your admin user if you're not already logged in.
3. Under `SOCIAL ACCOUNTS` click add next to `Social applications`.
4. Set Provider to `Google`.
5. Set Name to `Google Multinet Oauth`.
6. Copy the Client ID from Google's cloud dashboard to the MultiNet instance.
7. Copy the Client secret to `secret key` from Google's cloud dashboard to the MultiNet instance.
8. Add multinet.test to chosen sites.
9. Click save.

### Client
No changes should be necessary from the OAuth setup steps from above, but they must be completed.
When logging in, choose login with Google.

## Remap Service Ports (optional)
Attached services may be exposed to the host system via alternative ports. Developers who work
on multiple software projects concurrently may find this helpful to avoid port conflicts.

To do so, before running any `docker-compose` commands, set any of the environment variables:
* `DOCKER_POSTGRES_PORT`
* `DOCKER_RABBITMQ_PORT`
* `DOCKER_MINIO_PORT`

The Django server must be informed about the changes:
* When running the "Develop with Docker" configuration, override the environment variables:
  * `DJANGO_MINIO_STORAGE_MEDIA_URL`, using the port from `DOCKER_MINIO_PORT`.
* When running the "Develop Natively" configuration, override the environment variables:
  * `DJANGO_DATABASE_URL`, using the port from `DOCKER_POSTGRES_PORT`
  * `DJANGO_CELERY_BROKER_URL`, using the port from `DOCKER_RABBITMQ_PORT`
  * `DJANGO_MINIO_STORAGE_ENDPOINT`, using the port from `DOCKER_MINIO_PORT`

Since most of Django's environment variables contain additional content, use the values from
the appropriate `dev/.env.docker-compose*` file as a baseline for overrides.

## Testing
### Initial Setup
tox is used to execute all tests.
tox is installed automatically with the `dev` package extra.

When running the "Develop with Docker" configuration, all tox commands must be run as
`docker-compose run --rm django tox`; extra arguments may also be appended to this form.

### Running Tests
Run `tox` to launch the full test suite.

Individual test environments may be selectively run.
This also allows additional options to be be added.
Useful sub-commands include:
* `tox -e lint`: Run only the style checks
* `tox -e type`: Run only the type checks
* `tox -e test`: Run only the pytest-driven tests

To automatically reformat all code to comply with
some (but not all) of the style checks, run `tox -e format`.
