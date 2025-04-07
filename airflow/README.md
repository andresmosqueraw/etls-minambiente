## Ejecutar tests localmente

### Clonar repositorio

```bash
git clone https://github.com/ceicol/ETL_RL2.git
```

### Crear ambiente

```bash
cd ETL_RL2/airflow/
python3 -m venv .venv
```

### Activar ambiente

```bash
source .venv/bin/activate
```

### Instalar dependencias

```bash
pip install -r ./requirements.txt
```

### Ejecutar los tests localmente

```bash
coverage run -m unittest discover -s tests
coverage report -m
coverage xml
```

## Ejecutar tests usando docker

### Clonar repositorio

```bash
git clone https://github.com/ceicol/ETL_RL2.git
```

###  Ejecutar docker compose

```bash
cd ETL_RL2/airflow
docker compose up -d
```

### Ejecutamos los tests

```bash
docker compose run --rm test-runner
```

o

```bash
docker run -it --entrypoint bash my_airflow_image:latest -c "coverage run -m unittest discover -s /app/tests && coverage report -m && coverage xml && echo 'Tests finalizados.'"
```

### Ejecutamos los tests solamente para un archivo

```bash
coverage run -m unittest tests.test_data_utils
```



# Ejecutar tests

## Ejecutar tests individualmente
```bash
cd /opt/cpi/test/ETL_RL2/airflow
python3 -m unittest -b tests.test_utils
```
## Ejecutar todos los tests de rl2
```bash
python3 -m unittest discover -s tests -t .
```

## Generar resumen de cobertura de tests
```bash
coverage run -m unittest discover -s tests -t .
coverage report -m
```
## Generar resumen de cobertura de tests en xml y html
```bash
coverage xml
coverage html
```