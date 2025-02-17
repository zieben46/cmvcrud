from app.models.base_model import BaseModel, CrudType

class CSVModel(BaseModel):

    def __init__(self, csv_path):
        self.csv_path = csv_path


    def execute(self, operation: CrudType, **kwargs):
        pass