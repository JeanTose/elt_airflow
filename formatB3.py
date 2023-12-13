from src.repository import fileRepository
from src.repository import marketTypeRepository
from src.repository import bdiRepository
from src.repository import priceCorrectionRepository
from src.repository import companyRepository
from src.repository import paperRepository
from src.repository import pregaoRepository
from src.service import fileService

import os
import pendulum
from airflow.decorators import dag, task

@dag(
    schedule='0 */6 * * * ',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pievi"],
)

def finalFunction():   
    @task
    def getFiles():
        
        anos = [2019, 2020]
        files_path = []
        for ano in anos:
            name_file = f'COTAHIST_A{ano}.TXT'
            file_path = os.path.join('/opt/airflow/dags/src/data', name_file)
            files_path.append(file_path)

        formated_files = []
        for file_path in files_path:
            formattedFile = fileService.fommaterb3(file_path)
            formated_files.append(formattedFile)

        return formated_files
        
    @task
    def createStage(files_path):
        conn = fileRepository.connectBdd()
        fileRepository.drop_table(conn)
        fileRepository.create_table(conn)

        conn.commit()
        conn.cursor().close()

        for file_path in files_path:
            conn = fileRepository.connectBdd()
            engine = fileRepository.create_enginer()
            file_path.to_sql('btres', engine, if_exists='append', index=False)
            
            conn.commit()
            conn.cursor().close()
            
    @task
    def createStarSchema():
        conn = fileRepository.connectBdd()

        marketTypeRepository.create_table_market_type(conn)
        marketTypeRepository.insert_table_market_type(conn)

        bdiRepository.create_table_bdi(conn)
        bdiRepository.insert_table_bdi(conn)
    
        priceCorrectionRepository.create_table_price_correction(conn)
        priceCorrectionRepository.insert_table_price_correction(conn)

        companyRepository.create_table_company(conn)
        companyRepository.insert_table_company(conn)

        paperRepository.create_table_paper(conn)
        paperRepository.insert_table_paper(conn)

        pregaoRepository.create_table_pregao(conn)
        pregaoRepository.insert_table_pregao(conn)

        pregaoRepository.create_relation(conn)


    files_path = getFiles()
    stage = createStage(files_path)
    starSchema = createStarSchema()

    files_path >> stage >> starSchema


finalFunction()