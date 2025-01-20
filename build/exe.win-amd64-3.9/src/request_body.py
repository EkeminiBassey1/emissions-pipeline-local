# The above code defines several Pydantic models for representing geographic coordinates, zone points,
# route points, feature parameters, and a request body structure.
from typing import List, Optional
from pydantic import BaseModel


class Koordinaten(BaseModel):
    x: float
    y: float


class Zonenpunkte(BaseModel):
    land: str
    plzZone: str


class Routenpunkte_zonenpunkt(BaseModel):
    typ: str
    zonenpunkt: Optional[Zonenpunkte] = None


class Routenpunkte_koordinaten(BaseModel):
    typ: str
    koordinaten: Optional[Koordinaten] = None


class FeatureParameter(BaseModel):
    polygon: bool
    fahrzeit: bool
    maut: bool


class RequestBody(BaseModel):
    archive: bool
    routenpunkte: List
    orgeinheit: Optional[int] = None
    maxResults: Optional[int] = None
    featureParameter: FeatureParameter
    laendersperren: Optional[str] = None