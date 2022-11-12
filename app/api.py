
import os
from fastapi import FastAPI ,Query, Depends
from typing import Optional, List
from pydantic import BaseModel
from app.get_category import check_FTP_connection, get_latest_File, latest_run
from app.database import Base, engine, SessionLocal
from sqlalchemy import Column, String, Integer,Boolean
from sqlalchemy.orm import Session

# model
class GetCred(Base):
    __tablename__ = "Sftp_cred"
 
    host = Column(String)
    username = Column(String, primary_key=True)
    password = Column(String)
    port = Column(Integer)

    class Config:
        orm_mode= True
# schema
class SftpSchema(BaseModel):
    host:str = None
    username:str = None
    password:str = None
    port:int = None

def get_db():
    db =SessionLocal()
    try:
        yield db
    finally:
        db.close()


Base.metadata.create_all(bind=engine)

data_extraction = FastAPI()
@data_extraction.get("/home/")
def read_root():
   return {"Status": "UP"}

@data_extraction.post("/get_sftp_cred")
def get_sftp_cred(get_data:SftpSchema, db:Session=Depends(get_db)):
    try:
        exist_user = db.query(GetCred).filter(GetCred.username ==get_data.username).first()
        print(exist_user.username)
        host = exist_user.host
        user = exist_user.username
        paswd = exist_user.password
    except:
        record = GetCred(host =get_data.host, username= get_data.username, password = get_data.password, port = get_data.port)
        db.add(record)
        db.commit()
        host = get_data.host
        user = get_data.username
        paswd = get_data.password
    _,uniq_cat = check_FTP_connection(host, user, paswd)
    cat = ','.join(uniq_cat)
    return {"categories": cat}

@data_extraction.post("/get_user_input")
def get_user_input(get_data:SftpSchema, db:Session=Depends(get_db)):
    try:
        exist_user = db.query(GetCred).filter(GetCred.username ==get_data.username).first()
        print(exist_user.username)
        host = exist_user.host
        user = exist_user.username
        paswd = exist_user.password
    except:
        record = GetCred(host =get_data.host, username= get_data.username, password = get_data.password, port = get_data.port)
        db.add(record)
        db.commit()
        host = get_data.host
        user = get_data.username
        paswd = get_data.password
    _,uniq_cat = check_FTP_connection(host, user, paswd)
    cat = ','.join(uniq_cat)
    return {"categories": cat}


    
