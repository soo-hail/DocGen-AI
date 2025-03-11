# Manage-Document Page.
from openai import AsyncOpenAI
import asyncio
from PyPDF2 import PdfReader
from io import BytesIO
from itertools import chain
from asyncio import sleep
from peewee import SQL, JOIN, NodeList
import streamlit as st
from pydantic import BaseModel

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
 
from db import DocumentInformationChunks, DocumentTags, Tags, db, Documents, set_openai_api_key
from constants import CREATE_FACT_CHUNKS_SYSTEM_PROMPT, GET_MATCHING_TAGS_SYSTEM_PROMPT
from utils import find

# FUNCTION TO DELETE DOCUMENT FROM DATABSE.
def delete_document(document_id: int):
    Documents.delete().where(Documents.id == document_id).execute()
    
# BUILD USER-INTERFACE USING STREAMLIT.
st.set_page_config(page_title = "Manage Documents")
st.title("Manage Documents")

# 1. EXTRACT FACTS(FACTUAL INFORMATION) OUT OF CHUNKS.
IDEAL_CHUNK_LENGTH = 4000

# CLASS DEFINED USING 'pydantic' LIBRARY, COMMANLY USED FOR DATA-VALIDATION AND PARSING.
class GeneratedDocumentInformationChunks(BaseModel): 
    facts: list[str]
    
async def generate_chunks(index: int, pdf_text_chunk: str):
    # REQUEST OPENAI ATLEAST 5 TIMES TO GENERATE CHUNKS(FOCUDES ON KEY-INFORMATION).
    count_request = 0
    # TRY SENDING REQUEST TO GPT, IF IT FAILS(ERROR) ----> TRY ATLEAST 5 TIMES WITH 2SEC AWAIT.
    while(True):
        try:
            # INTERACT WITH OPENAI API.
            client = AsyncOpenAI()
            output = await client.chat.completions.create(
                model = 'gpt-4o-mini-2024-07-18',
                messages = [{'role': 'system', 'content': CREATE_FACT_CHUNKS_SYSTEM_PROMPT},
                            {'role': 'user', 'content': pdf_text_chunk}],
                temperature = 0.1, # LESS RANDOMNESS(SAFE ANSWER)
                top_p = 1,
                frequency_penalty = 0,
                presence_penalty = 0,
            )
            
            # IF FACTS ARE NOT GENERATED.
            if not output.choices[0].message.content:
                raise Exception('No facts generated')
            
            # PARSE AND EXTRACT THE FACTS AS A STRING(FROM JSON FORMATE) FROM GENERATED OUTPUT.
            document_information_chunks = GeneratedDocumentInformationChunks.model_validate_json(output.choices[0].message.content).facts
            # ".model_validate_json()" PARSES THE OUTPUT(JSON) AND CONVERTS IT AS ATTRIBUTE DEFINED IN 'GeneratedDocumentInformationChunks' CLASS.
            # EG: {'FACTS': [FACT-1, FACT-2, FACT-3]} --------> FACTS = [FACT-1, FACT-2, FACT-3]
            print(f"Generated {len(document_information_chunks)} facts for pdf text chunk {index}.")

        except Exception as e:
            count_request += 1
            
            if count_request >= 5:
                raise e # AFTER 5 REQUESTS, RAISE ERROR.
            
            await sleep(2) # AWAIT 2SEC - BEFORE SENDING OTHER REQUEST.
            print(f"Failed to generate facts for pdf text chunk {index} with this err: {e}. Retrying...")

# 2. MATCH THE TAG FOR DOCUMENT(TO ASSIGN A TAG FOR A DOCUMENT).

class GeneratedMatchingTags(BaseModel): # FOR VALIDATING AND PARSING THE DATA.
    tags: list[str]

async def get_matching_tags(pdf_text: str):
    tags_result = Tags.select() # EXTRACT ALL THE TAGS FROM 'TAGS-TABLE' IN DATABASE.
    tags = [tag.lower() for tag in tags_result]
    
    # IF THERE ARE NO TAGS IN 'TAGS-TABLE' IN DATABASE
    if not len(tags):
        return []
    
    # USE LLM(MODLES) TO MATCH THE TAGS FOR DOCUMENT BASED ON CONENT OF A DOCUMENT.
    count_requests = 0
    while True:
        try:
            # INTERACT WITH OPENAI API TO MATCH THE TAG.
            client = AsyncOpenAI()
            output = await client.chat.completions.create(
                model="gpt-4o-mini-2024-07-18",
                messages=[
                    {
                        "role": "system",
                        "content": GET_MATCHING_TAGS_SYSTEM_PROMPT.replace("{{tags_to_match_with}}", str(tags))
                    },
                    {
                        "role": "user",
                        "content": pdf_text
                    }
                ],
                temperature=0.1,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0,
                response_format={
                    "type": "json_object"
                }
            )

            # IF RESPONSE HAS NOT GENERATED. 
            if not output.choices[0].message.content:
                raise Exception("Empty response for generating matching tags.")
            
            # VALIDATE, PARSE AND EXTRACT THE MATRCHING-TAG.
            matching_tag_names = GeneratedMatchingTags.model_validate_json(output.choices[0].message.content).tags
            
            # EXTRACT THE ID(OF TAG) - GET MATCHING-TAG ID.
            matching_tag_ids: list[int] = []
            for tag_name in matching_tag_names:
                # FIND IS A FUNCTION() THAT TAKES A FUNCTION, AND ITERBALE-VARIABLE(TAGS) AS INPUT AND RETURNS A MATCHING TAG(ROW OF TAGS-TABLE).
                matching_tag = find(lambda tag: tag.name.lower() == tag_name.lower(), tags_result)
                if matching_tag:
                    matching_tag_ids.append(matching_tag.id)
                else:
                    raise Exception(f"Tag {tag_name} matched not found in database.")
                
            print(f"Generated matching tags {str(matching_tag_names)} for pdf text.")
            return matching_tag_ids
                    
        except Exception as e:
            # TRY REQUESTING OPENAI API FOR 5 TIMES, INCASE SOME ERROR OCCURED WHILE INTERACTING WITH OPENAI API. 
            count_requests += 1
            if count_requests > 5:
                raise e
            
            await sleep(2) # 2SEC AWAIT(DELAY) FOR NEXT REQUEST.
            
# 3. UPLOAD DOCUMENT FUNCTION - EXTRACT FACTS, MATCHING-IDS AND STORE ALL THE INFORMATION IN DATABASE.
def upload_document(name: str, pdf_file: bytes):
    # Load the PDF file from BytesIO

    reader = PdfReader(BytesIO(pdf_file))
    # Extract text from all pages and join with double newlines
    pdf_text = "\n\n".join([page.extract_text() for page in reader.pages])
    
    # DIVIDE THE PDF-TEXT INTO CHUNKS. 
    pdf_text_chunks: list[str] = []
    for i in range(0, len(pdf_text), IDEAL_CHUNK_LENGTH):
        pdf_text_chunks.append(pdf_text[i:i + IDEAL_CHUNK_LENGTH])
    
    # PREPARE THE LIST OF TASKS - TO RUN ALL OF THEM AT ONCE.
    # NOTE: generate_chunks IS A ASYNCRONUS FUNCTION - SO IT IS POSSIBLE TO STORE AS A TASK.
    generate_chunks_coroutines = [generate_chunks(index, pdf_text_chunk) for index, pdf_text_chunk in enumerate(pdf_text_chunks)] # STORE TASKS, TASK(GENEARTE_CHUNKS)   
    # THIS ALLOWS TO PERFORM MANY THINGS AT ONCE, INSTEAD OF ONE BY ONE - ABOVE TASKS(generate_chunks_coroutines) ARE EXECUTED AT ONCE INDTEAD OF ONE-BY-ONE.
    event_loop = asyncio.new_event_loop() # CREATE NEW-EVENT-LOOP(EVENT-LOOP IS A SCHEDULER), THAT WILL HANDLE EXECUTING THE TASKS.
    asyncio.set_event_loop(event_loop) # SET EVENT-LOOP.
    generate_chunks_coroutines_gather = asyncio.gather(*generate_chunks_coroutines) # GATHER ALL THE TASKS IN 'generate_chunks_coroutines'. 
    # NOTE:
    # THE * MEANS YOU UNPACK THE LIST OF TASKS AND PASS THEM INDIVIDUALLY INTO ASYNCIO.GATHER.
    # ASYNCIO.GATHER TAKES ALL THE TASKS (COROUTINES) YOU’VE STORED IN GENERATE_CHUNKS_COROUTINES AND TELLS THE EVENT LOOP TO RUN THEM ALL AT THE SAME TIME.
    get_matching_tags_coroutine = get_matching_tags(pdf_text[0:5000])
    
    # RUN THE TWO-TASKS(GENERATE-CHUNKS, GET-MATCHING-TAGS) WITH EVENT-LOOP.
    document_information_chunks_from_each_pdf_text_chunk, matching_tag_ids = event_loop.run_until_complete(asyncio.gather(generate_chunks_coroutines_gather, get_matching_tags_coroutine))

    # FLATTEN ALL-CHUNKS(LIST OF LIST) INTO A LIST.
    # EG: [[1], [2], [3]] ---> [1, 2, 3]
    document_information_chunks = list(chain.from_iterable(document_information_chunks_from_each_pdf_text_chunk))
    
    # STORE DATA IN DATA-BASE.
    # DB.ATOMIC() IS USED TO GROUP MULTIPLE DATABASE ACTIONS INTO ONE "BLOCK" OR "TRANSACTION."
    # TRANSACTION MEANS ALL OR NOTHING: EITHER EVERYTHING INSIDE THE BLOCK IS SAVED, OR IF SOMETHING GOES WRONG, NOTHING GETS SAVED - NICE RIGHT?
    
    with db.atomic() as transaction:
        set_openai_api_key()
        
        # INSERT INTO DOCUMENTS-TABLE.
        document_id = Documents.insert(
            name = name,
        ).execute()
        
        # INSERT INTO 'DocumentInformationChunks'
        DocumentInformationChunks.insert_many(
            # NOTE: ALSO WE ARE EXECUTING OPENAI DIRECTLY IN SQL.
             [{"document_id": document_id, "chunk": chunk, "embedding": SQL(f"ai.openai_embed('text-embedding-3-small', %s)", (chunk,))} for chunk in document_information_chunks]
        ).execute()
        
        # INSERT INTO 'DocumentTags'
        DocumentTags.insert_many(
            [{"document_id": document_id, "tag_id": tag_id} for tag_id in matching_tag_ids]
        ).execute()
        
        transaction.commit() # SAVE WHOLE TRANSITION(OR BLOCK)
        print(f"Inserted {len(document_information_chunks)} facts for pdf {name} with document id {document_id} and {len(matching_tag_ids)} matching tags.")
    event_loop.close()

@st.dialog("Upload document") # CREATES A DIALOG BOX IN STREAMLIT
def upload_document_dialog_open(): # THIS FUNCTION IS TRIGGERED WHEN A DIALOG-BOX IS OPENED.
    pdf_file = st.file_uploader("Upload PDF file", type="pdf")
    
    if pdf_file is not None:
        if st.button("Upload", key="upload-document-button"): # CREATE A BUTTON.
            upload_document(pdf_file.name, pdf_file.getvalue()) # FUNCTION-CALL.
            st.rerun() # RELOAD THE PAGE AFTER UPLOADING THE FILE.
            
st.button("Upload Document", key="upload-document-button", on_click=upload_document_dialog_open)

documents = Documents.select(
    Documents.id,
    Documents.name,
    # REMOVES THE NULL VALUES FROM LIST OF TAG-NAMES.
    NodeList([
        SQL('array_remove(array_agg('),
        Tags.name,
        SQL('), NULL)')
    ]).alias("tags")
    
).join(DocumentTags, JOIN.LEFT_OUTER).join(Tags, JOIN.LEFT_OUTER).group_by(Documents.id).execute()

if len(documents):
    for document in documents:
        document_container = st.container(border=True)
        document_container.write(document.name)
        if len(document.tags):
            document_container.write(f"Tags: {', '.join(document.tags)}")
        
        document_container.button("Delete", key=f"{document.id}-delete-button", on_click = lambda: delete_document(document.id))

else:
     st.info("No documents are uploaded yet.")