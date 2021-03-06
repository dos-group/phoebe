import sklearn
from fastapi import FastAPI, status
from fastapi.responses import JSONResponse


def start_server():
    from app.latency.routes import latency_router, latency_metadata
    from app.common.routes import common_router, common_metadata
    from app.recoverytime.routes import recoverytime_router, recoverytime_metadata
    from app.workload.routes import workload_router, workload_metadata
    from app.baselines.routes import baselines_router, baselines_metadata

    my_app = FastAPI(
        title="Phoebe-Py",
        description="Lightweight python service for handling predictions in Phoebe.",
        version="1.0.0",
        openapi_tags=[common_metadata, latency_metadata,
                      workload_metadata, recoverytime_metadata, baselines_metadata]
    )
    # routes
    my_app.include_router(common_router)
    my_app.include_router(latency_router)
    my_app.include_router(recoverytime_router)
    my_app.include_router(workload_router)
    my_app.include_router(baselines_router)

    @my_app.exception_handler(sklearn.exceptions.NotFittedError)
    async def not_fitted_error_handler(*args, **kwargs):
        return JSONResponse(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            content={"message": "The required model was not yet trained!"},
        )

    # return app
    return my_app


app = start_server()
