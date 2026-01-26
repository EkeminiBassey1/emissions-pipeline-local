# The above code defines several Pydantic models for representing geographic coordinates, zone points,
# route points, feature parameters, and a request body structure.
from typing import List, Optional
from pydantic import BaseModel


class Koordinaten(BaseModel):
    longitude: float
    latitude: float


class Zonenpunkte(BaseModel):
    countryIsoCode: str
    postalCode: str


class Routenpunkte_zonenpunkt(BaseModel):
    address: Optional[Zonenpunkte] = None

class Routenpunkte_koordinaten(BaseModel):
    koordinaten: Optional[Koordinaten] = None


class FeatureParameter(BaseModel):
    polygon: bool
    tollInformation: bool
    maneuverEvents: bool
    maneuverEventsLanguage: str
    archive: bool
    routeSegments: bool

class RequestBody(BaseModel):
    waypoints: List
    orgUnit: Optional[int] = None
    features: FeatureParameter
    maxResults: Optional[int] = None
    laendersperren: Optional[str] = None