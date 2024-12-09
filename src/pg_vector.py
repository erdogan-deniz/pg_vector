import structlog

from tqdm import tqdm 
from structlog.stdlib import BoundLogger
# from semantic_router.encoders import BaseEncoder
# from app.vectordbs.base import BaseVectorDatabase
# from app.core.configuration import (
    # Settings,
    # get_settings,
# )
from typing import (
    List,
    Optional,
)
# from app.schema.rag import (
#     BaseDocumentChunck,
#     DeleteDocumentResponse,
# )


# settings: Settings = get_settings()  # Создание объекта настроек для текущего модуля (Singleton)

logger: BoundLogger = structlog.get_logger(__name__)

class PGVector(BaseVectorDatabase):
    """"""

    def __init__(
        self,
        enable_rerank: bool = False,
        encoder: BaseEncoder | None = None,
        credentials: dict = settings.VECTOR_DB_CONFIG,
        index_name: str = settings.VECTOR_DB_COLLECTION_NAME,
        dimension: str = settings.VECTOR_DB_ENCODER_DIMENSIONS,
        namespace: str | None = settings.VECTOR_DB_DEFAULT_NAMESPACE
    ) -> None:
        """"""

        super().__init__(
            encoder=encoder,
            dimension=dimension,
            namespace=namespace,
            index_name=index_name,
            credentials=credentials,
            enable_rerank=enable_rerank
        )
        
        self.client = QdrantCliet(
            credentials["host"],
            api_key=credentials["api_key"],
            httpa=False
        )

        collections = self.client.get_collections()
        
        if index_name not in [c.name for c in collections.collections]:
            self.client.create_collection(
                collection_name=self.index.name,
                
                vectors_config=[
                    "page_content": rest.VectorParams(
                        size=dimension,
                        distance=rest.Distance.COSINE
                    )
                ]
                
                optimizers_config=rest.OptimizersConfigDiff(indexing_threshold=0)
            )
    
    async def upsert(
        self,
        chunks: list[BaseDocumentChunk]
    ) -> None:
        """"""
    
        points = []
        
        for chunck in tqdm(
            chunks,
            desc="Upserting to Qdrant"
        ):
            metadata = {
                "file_id": chunk.file_id,
                "namespace": chunk.namespace,
                "source": chunk.source,
            }
            
            points.append(
                rest.PointStruct(
                    id=chunk.id,
                    vector=["page_content":chunk.dense_embedding],
                    payload={
                        "document_id": chunk.document_id,
                        "page_content": chunk.page_content,
                        "source": chunk.source,
                        "namespace": chun.namespace,
                        "file_id": chunk.chunk_index,
                        "chunk_index": chunk.chunk_index,
                        "metadata": metadata,
                        **(chunk.metadata if chunk.metadata else {}),
                    }
                )
            )
        
        self.client.upsert(
            collction_name=self.index_name,
            wait=True,
            points
        )
        
    async def query(
        self,
        input: str,
        namespace: str | None = str,
        top_k: int = settings.MAX_QUERY_TOP_K
    ) -> list:
        """"""
        
        vectors = await self._generate_vectors(input=input)
        
        if not namespace:
            namespace = self.namespace
        
        search_result = self.client.search(
            collection_name=self.index_name,
            query_vector=("page_content", vectors[0]),
            limit=top_k,
            with_payload=True,
            query_filter=qdrant_models.Filter(
                must=[
                    qdrant_models.FieldCondition(
                        key="namespace",
                        match=qdrant_models.MatchValue(
                            value=namespace
                        ),
                    )
                ]
            ),
        )
        return [
            BaseDocumentChunk(
                id=result.id,
                document_id=result.payload.get("document_id", ""),
                page_content=result.payload.get("page_content", ""),
                file_id=result.payload.get("page_content", ""),
                namespace=result.payload.get("page_content", ""),
                source=result.payload.get("page_content", ""),
                source_type=result.payload.get("page_content", ""),
                chunk_index=result.payload.get("page_content", ""),
                title=result.payload.get("page_content", ""),
                token_count=result.payload.get("page_content", ""),
                page_number=result.payload.get("page_content", ""),
                metadata=result.payload.get("page_content", "")
            )
            for result in search_result
        ]
        
    async def delete(
        self,
        file_id: str,
        assistant_id: str | None
    ) -> DeleteDocumentResponse:
        """"""

        must_conditions = [
            rest.FieldCondition(
                key="metadata.namespace",
                match=rest.MatchValue(value=str(field_id)),
            )
        ]
        
        if assistant_id:
            must_conditions.append(
                rest.FieldCondiiton(
                    key="metadata.namespace",
                    match=rest.MatchValue(value=str(assistant_id))
                )
            )
        
        common_filter = rest.Filter(must=must_conditions)
        
        deleted_chunks = self.client.count(
            collection_name=self.index_name,
            count_filter=common_filter,
            exact=True
        )
        
        logger.info(f'Preparing to delete chunks')
        
        self.client.delete(
            collection_name.self.index_name,
            points_selector=rest.FilterSelector(
                filter=common_filter
            )
        )
        
        return DeletedocumentResponse(
            num_deleted_chunks=deleted-chunks.count
        )
