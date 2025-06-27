"""
Модели для API

"""
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class GenerationRequest(BaseModel):
    """Модель запроса на генерацию данных"""

    num_clients: int = Field(
        default=1000, ge=1, le=10000000, description="Количество клиентов"
    )
    num_certificates: int = Field(
        default=2000, ge=1, le=20000000, description="Количество сертификатов"
    )
    batch_size: Optional[int] = Field(
        default=None, ge=100, description="Размер батча (автоматически если не указан)"
    )
    num_workers: Optional[int] = Field(
        default=None, ge=1, le=32, description="Количество рабочих потоков"
    )
    write_buffer_size: Optional[int] = Field(
        default=None, ge=100, description="Размер буфера записи"
    )


class GenerationStatus(BaseModel):
    """Модель статуса генерации"""

    is_running: bool
    progress: int
    total: int
    current_stage: str
    start_time: Optional[datetime]
    error: Optional[str]
    estimated_time_remaining: Optional[int] = None


class ActiveCertificate(BaseModel):
    """Модель активного сертификата"""

    client_id: int
    certificate_id: str
    expiry_date: str
    days_until_expiry: int


class ActiveCertificatesResponse(BaseModel):
    """Модель ответа с активными сертификатами"""

    certificates: List[ActiveCertificate]
    total_count: int
    page: int
    page_size: int
    has_next: bool


class ClientCertificateAssignment(BaseModel):
    """Модель связи клиент-сертификат"""

    id: int
    client_id: int
    certificate_id: str
    expiry_date: str
    is_active: bool
    days_until_expiry: Optional[int] = None


class ClientCertificateAssignmentsResponse(BaseModel):
    """Модель ответа со связями клиент-сертификат"""

    assignments: List[ClientCertificateAssignment]
    total_count: int
    skip: int
    limit: int
    has_next: bool
