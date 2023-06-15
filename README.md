# Case de ETL usando PySpark e Delta Lake
Este case de ETL pode ser usando em qualquer Cloud Pública.

> Na minha máquina roda um Ubuntu 22.04 LTS. Então as instruções abaixo foram testadas neste ambiente.
> O código em PySpark que realiza o Job de ETL pode ser usado no Azure Databricks, GCP/DataProc, etc...
> Como não tenho uma conta de Teste na Azure para uso pessoal, fiz os testes num cluster Spark que tenho instalado localmente no meu Notebook.

## Instalando python, pip e git

```
sudo apt update
sudo apt install python3
sudo apt install python3-pip
sudo apt install git
```

## Instalando o Ambiente Virtual para rodar o projeto

```
 sudo apt update 
 sudo apt install python3-venv
 python3 -m venv etl_case_venv
 source etl_case_venv/bin/activate
```

## Instalando o Spark no Ubuntu 22.04

```
sudo apt update
sudo apt install default-jdk
wget https://dlcdn.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz
tar xvf spark-3.4.0-bin-hadoop3.tgz
sudo mv spark-3.4.0-bin-hadoop3 /opt/spark
vim ~/.bashrc
# Adicione as linhas abaixo no final do arquivo .bashrc
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin

source ~/.bashrc
```
## Clonando o repositório

````
git clone https://github.com/oraculodata/etl_case.git 
cd etl_case
````

## Instalando as dependências 

```
pip install -r requirements.txt
```


## Execute o Job de ETL

```
python etl_case.py
```

## Notebook Jupyter

Você pode usar também o arquivo [etl_case.ipynb](https://github.com/oraculodata/etl_case/blob/main/etl_case.ipynb) 


