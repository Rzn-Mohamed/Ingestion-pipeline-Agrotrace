from pydantic import BaseModel
from typing import Optional
import datetime

class CapteurData(BaseModel):
    capteur_id: str
    timestamp: datetime.datetime
    temperature: Optional[float] = None
    humidite: Optional[float] = None
    humidite_sol: Optional[float] = None
    niveau_ph: Optional[float] = None
    luminosite: Optional[float] = None