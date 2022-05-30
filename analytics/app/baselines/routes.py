from fastapi import APIRouter, status, BackgroundTasks, Depends, HTTPException

from app.baselines.schemes import *
from app.baselines.models import TWRESModelProvider
from app.common.logging_utils import log_request
from app.common.models import BasePredictionModel
from app.common.schemes import ScheduledTaskResponse
from app.workload.models import WorkloadModelProvider

baselines_router = APIRouter(
    prefix="/baselines",
    tags=["baselines"]
)

baselines_metadata: dict = {
    "name": "baselines",
    "description": "Endpoints for baseline methods."
}


@baselines_router.post("/twres_training",
                       response_model=ScheduledTaskResponse,
                       name="Train the TWRES-baseline.",
                       status_code=status.HTTP_200_OK)
async def train_twres_model(request: TWRESTrainingRequest,
                            background_tasks: BackgroundTasks,
                            model: BasePredictionModel = Depends(TWRESModelProvider.get_instance())):
    log_request(request, model)
    background_tasks.add_task(model.fit, request)
    return ScheduledTaskResponse(message="Added twres-model to the list of background tasks.",
                                 task_hash=hash(background_tasks.tasks[-1]))


@baselines_router.post("/twres_prediction",
                       response_model=TWRESPredictionResponse,
                       name="Predict optimal scale-out with TWRES-baseline.",
                       status_code=status.HTTP_200_OK)
async def predict_with_twres_model(request: TWRESPredictionRequest,
                                   model: BasePredictionModel = Depends(TWRESModelProvider.get_instance()),
                                   workload_model: BasePredictionModel = Depends(WorkloadModelProvider.get_instance())):
    log_request(request, model)
    # sanity check
    if not model.is_fitted:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED,
                            detail=f"{model.__class__.__name__} has not been fitted!")
    if not workload_model.is_fitted:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED,
                            detail=f"{workload_model.__class__.__name__} has not been fitted!")

    scale_out: int = model.predict(request, workload_model)
    return TWRESPredictionResponse(scale_out=scale_out)
