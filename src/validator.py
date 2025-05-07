from pydantic import BaseModel, EmailStr, Field, field_validator
from typing import List, Optional
import re

class EmailPayload(BaseModel):
    subject: str = Field(..., min_length=1, description="Subject of the email")
    message1: str = Field(..., min_length=1, description="First part of the email message")
    message2: Optional[str] = Field(None, description="Second part of the email message (optional)")
    name: Optional[str] = Field(None, description="Email receiver name(optional)")
    email: EmailStr = Field(..., description="Recipient's email address")

class SMSPayload(BaseModel):
    message: str = Field(..., min_length=1, description="SMS message content")
    phoneNumber: str = Field(..., description="Recipient's phone number")

    @field_validator('phoneNumber')
    @classmethod
    def validate_phone_number(cls, v: str) -> str:
        # Basic validation for Nigerian phone numbers
        if not re.fullmatch(r'(\+234|0)[789][01]\d{8}', v):
            raise ValueError('Invalid Nigerian phone number format')
        return v

class PushPayload(BaseModel):
    message: str = Field(..., min_length=1, description="Push notification message")
    oneSignalIds: List[str] = Field(..., min_items=1, description="List of OneSignal recipient IDs")
    actionName: str = Field(..., min_length=1, description="Action name associated with the notification")
