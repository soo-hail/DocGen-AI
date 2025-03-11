import asyncio
import streamlit as st
from asyncio import sleep
from peewee import SQL
from typing import Literal, Optional, TypedDict, Union

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
 
from constants import RESPOND_TO_MESSAGE_SYSTEM_PROMPT
from db import DocumentInformationChunks, set_diskann_query_rescore, set_openai_api_key, db

st.set_page_config(page_title = "Chat With Documents")
st.title('Chat With Documents')

# STORE MESSAGES(CHAT) OF A SESSSION - BECAUSE AFTER EVERY NEW MESSAGE(REFRESH), OLD MESSAGES DISAPPERS.
class Message(TypedDict): # DEFINES STURCTURE OF A MESSAGE.
    # 'TypedDict' - TO DEFINE VARIBALES LIKE DICT, 'TYPE-SAFE' DICT(SO WE ARE INHERITING THAT CLASS).
    
    # ATTRIBUTES:
    role: Union[Literal["user"], Literal["assistant"]] # 'ROLE' MUST HAVE A VALUE EITHER 'USER' OR(UNIION) 'ASSISTANT'.
    content: str # STORE MESSAGE-TEXT(STRING TYPE)
    references: Optional[list[str]] # CHUNKS USED TO ANSWER.

if 'messages' not in st.session_state:
    # TO STORE SESSION-HISTORY.
    st.session_state['messages'] = []
    
# TO PUSH MESSAGE IN SESSION.
def push_message(message: Message):
    # UPDATE THE NEW MESSAGE.
    st.session_state["messages"] = [*st.session_state["messages"], message] # APPEND NEW MESSAGE.
    
async def send_message(input_message: str):
    
    related_document_information_chunks: list[str] = [] # TO STORE RETRIVED CHUNKS RELATED TO USER QUERY.
    with db.atomic() as transaction:
        set_openai_api_key()
        # RETRIEVE TOP-5 RELAVENT CHUNKS TO 'INPUT_MESSAGE(USER QUERY)'.
        result = DocumentInformationChunks.select().order_by(SQL(f"embedding <-> ai.openai_embed('text-embedding-3-small',%s)", (input_message,))).limit(5).execute()

        # STORE CHUNKS.
        for row in result:
            related_document_information_chunks.append(row.chunk)
            
        transaction.commit()
        
        push_message({'role': 'user', 'content': input_message, 'references': related_document_information_chunks})
        
        request_count = 0
        while True:
            try:
                pass
            except Exception as e:
                request_count = request_count + 1

                if request_count >= 5:
                    raise e 

                # REQUEST GPT 5 TIMES - SOME TIMES WE GET ERROR WHILE REQUESTING.
                await sleep(2)
                print(f"Failed to generate response with this err: {e}. Retrying...")
                
        st.rerun() # RERUN THE PAGE.
        
input_message = st.chat_input("Message ChatGPT")
if input_message:
    # EVENT-LOOP(SCHEDULER) TO RUN ASYNIC FUNCTIONS(PARALELLY) 
    event_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(event_loop)
    event_loop.run_until_complete(send_message(input_message))
    event_loop.close()