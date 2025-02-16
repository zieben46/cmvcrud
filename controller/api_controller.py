from fastapi import FastAPI, Depends, HTTPException, Query
from model.db_model import DatabaseModel  # Your DB handler
from model.base_model import CrudType, SCDType
from fastapi.responses import StreamingResponse
import io
import os
import pandas as pd

from model.base_model import DatabaseType


app = FastAPI()


def get_db_model(table_name: str):
    return DatabaseModel(DatabaseType.POSTGRES, table_name)

class APIController:
    
    @staticmethod
    @app.post("/{table_name}/create")
    def create_entry(
        table_name: str, 
        scd_type: SCDType, 
        data: dict, 
        db: DatabaseModel = Depends(get_db_model)
    ):
        """Receives user input and triggers CREATE operation."""
        try:
            db.execute(CrudType.CREATE, scd_type, **data)
            return {"message": f"✅ Entry added to `{table_name}` with SCD type `{scd_type}`"}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @staticmethod
    @app.get("/{table_name}/read")
    def read_entries(
        table_name: str, 
        scd_type: SCDType, 
        skip: int = Query(0, ge=0), 
        limit: int = Query(100, le=1000), 
        db: DatabaseModel = Depends(get_db_model)
    ):
        """Reads entries with optional pagination."""
        try:
            df = db.execute(CrudType.READ, scd_type)
            paginated_df = df.iloc[skip: skip + limit]
            return paginated_df.to_dict(orient="records")
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @staticmethod
    @app.put("/{table_name}/update/{entry_id}")
    def update_entry(
        table_name: str, 
        entry_id: int, 
        scd_type: SCDType, 
        data: dict, 
        db: DatabaseModel = Depends(get_db_model)
    ):
        """Updates an entry in a specific table."""
        try:
            db.execute(CrudType.UPDATE, scd_type, id=entry_id, **data)
            return {"message": f"🔄 Entry `{entry_id}` updated in `{table_name}`"}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @staticmethod
    @app.delete("/{table_name}/delete/{entry_id}")
    def delete_entry(
        table_name: str, 
        entry_id: int, 
        scd_type: SCDType, 
        db: DatabaseModel = Depends(get_db_model)
    ):
        """Deletes an entry in a specific table."""
        try:
            db.execute(CrudType.DELETE, scd_type, id=entry_id)
            return {"message": f"🗑️ Entry `{entry_id}` deleted from `{table_name}`"}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    # @staticmethod
    # @app.get("/{table_name}/download")
    # def download_table(
    #     table_name: str, 
    #     scd_type: SCDType, 
    #     db: DatabaseModel = Depends(get_db_model)
    # ):
    #     """Streams table data as a downloadable CSV file."""
    #     try:
    #         df = db.execute(CrudType.READ, scd_type)
    #         stream = io.StringIO()
    #         df.to_csv(stream, index=False)
    #         stream.seek(0)
    #         return StreamingResponse(stream, media_type="text/csv", headers={"Content-Disposition": f"attachment; filename={table_name}.csv"})
    #     except Exception as e:
    #         raise HTTPException(status_code=400, detail=str(e))
