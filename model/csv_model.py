# from model.db_config import Config
# from model.db_model import Model
from model.base_model import BaseModel, CrudType, SCDType



class CSVModel(BaseModel):

    def __init__(self, csv_path):
        self.csv_path = csv_path


    def execute(self, operation: CrudType, scd_type: SCDType, **kwargs):
        pass