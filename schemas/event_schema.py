from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime
import uuid


class EcommerceEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    user_id: str
    product_id: str
    price: Optional[float] = None
    event_timestamp: datetime
    ingestion_timestamp: datetime

    @validator("event_type")
    def validate_event_type(cls, value):
        allowed = {"view", "add_to_cart", "purchase"}
        if value not in allowed:
            raise ValueError(f"event_type must be one of {allowed}")
        return value

    @validator("price")
    def validate_price(cls, value, values):
        if values.get("event_type") == "purchase":
            if value is None or value <= 0:
                raise ValueError("purchase events must have a positive price")
        return value
