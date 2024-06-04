# Databricks notebook source
!pip install -r requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC __Datasets:__
# MAGIC 1. Budget 2024: https://budget.canada.ca/2024/home-accueil-en.html#pdf

# COMMAND ----------

# Download file
import urllib

urls = ["https://budget.canada.ca/2024/report-rapport/budget-2024.pdf"
        ,"https://budget.canada.ca/2024/report-rapport/gdql-egdqv-en.pdf"
        ,"https://budget.canada.ca/2024/report-rapport/tm-mf-en.pdf"
        ]
for url in urls:
  filename = url.split("/")[-1]
  print(filename)
  # Get file
  urllib.request.urlretrieve(url, f"/tmp/{filename}")
  # Move data with dbutils
  dbutils.fs.mv(f"file:/tmp/{filename}", f"/Volumes/main/default/myfiles/{filename}")


# COMMAND ----------

# Create folder to store pdfs and results
dbutils.fs.mkdirs("/Volumes/main/default/myfiles/budget/")
dbutils.fs.mkdirs("/Volumes/main/default/myfiles/budget_results/")
len(dbutils.fs.ls("/Volumes/main/default/myfiles/budget/"))
#Delete folder (Recursive = True)
#dbutils.fs.rm("/Volumes/main/default/myfiles/budget/",True)

# COMMAND ----------

# Split PDF per page
from pypdf import PdfReader, PdfWriter

#def split_pdf(filename):
for url in urls:
    filename = url.split("/")[-1]
    print(filename)    
    filename_path = f"/Volumes/main/default/myfiles/{filename}"
    reader = PdfReader(filename_path)
    pages = reader.pages
    print(f"Number of pages:{len(pages)}")

    for i in range(len(pages)):
        page_filename = F"/Volumes/main/default/myfiles/budget/{filename.split('.')[0]}_{i}.pdf"
        with open(page_filename,"wb") as f:
            writer = PdfWriter()
            writer.add_page(pages[i])
            writer.write(f)
            f.seek(0)
       

len(dbutils.fs.ls("/Volumes/main/default/myfiles/budget/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read PDF as BInary files

# COMMAND ----------

df_pdfs = spark.read.format("binaryFile").load("/Volumes/main/default/myfiles/budget")
display(df_pdfs)

# COMMAND ----------

import sys
import os
import io
import glob
import json
import html 
import pandas as pd
#from PyPDF2 import PdfReader
#from markdownify import markdownify as md
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest, ContentFormat, AnalyzeResult

from azure.core.credentials import AzureKeyCredential
#from azure.identity import AzureDeveloperCliCredential
from azure.storage.blob import BlobServiceClient
import requests
from dotenv import load_dotenv
#load_dotenv("/Workspace/Users/ezzatdemnati@microsoft.com/config/ez_env.env")


def table_to_html(table):
    table_html = "<table>"
    rows = [
        sorted([cell for cell in table.cells if cell.row_index == i], key=lambda cell: cell.column_index)
        for i in range(table.row_count)
    ]
    for row_cells in rows:
        table_html += "<tr>"
        for cell in row_cells:
            tag = "th" if (cell.kind == "columnHeader" or cell.kind == "rowHeader") else "td"
            cell_spans = ""
            if cell.column_span > 1:
                cell_spans += f" colSpan={cell.column_span}"
            if cell.row_span > 1:
                cell_spans += f" rowSpan={cell.row_span}"
            table_html += f"<{tag}{cell_spans}>{html.escape(cell.content)}</{tag}>"
        table_html += "</tr>"
    table_html += "</table>"
    return table_html

def get_document_markdown(filename):
    document_intelligence_creds = AzureKeyCredential(os.getenv("AZURE_FORM_RECOGNIZER_KEY"))

    # Document Intelligence Output to markdown format
    document_intelligence_client = DocumentIntelligenceClient(endpoint=os.getenv("AZURE_FORM_RECOGNIZER_ENDPOINT"), 
                                                                credential=document_intelligence_creds)
    poller = document_intelligence_client.begin_analyze_document(
        "prebuilt-layout",
        AnalyzeDocumentRequest(url_source=filename),
        output_content_format=ContentFormat.MARKDOWN,
    )
    result: AnalyzeResult = poller.result()

    return result.content

def get_document_text_local(filename):
    offset = 0
    page_map = []
    reader = PdfReader(filename)
    pages = reader.pages
    for page_num, p in enumerate(pages):
        page_text = p.extract_text()
        page_map.append((page_num, offset, page_text))
        offset += len(page_text)
    return page_map


def get_document_text(filename):
    offset = 0
    page_map = []

    formrecognizer_creds = AzureKeyCredential(os.getenv("AZURE_FORM_RECOGNIZER_KEY"))
                            
    form_recognizer_client = DocumentAnalysisClient(
        endpoint=os.getenv("AZURE_FORM_RECOGNIZER_ENDPOINT"),
        credential=formrecognizer_creds,
        headers={"x-ms-useragent": "azure-search-chat-demo/1.0.0"},
    )
    
    # Process file from local folder
    with open(filename, "rb") as f:
        poller = form_recognizer_client.begin_analyze_document("prebuilt-layout", document=f)
    form_recognizer_results = poller.result()

    # Process file from Blob storage URL
    #poller = form_recognizer_client.begin_analyze_document_from_url("prebuilt-layout", document_url=filename)    

    form_recognizer_results = poller.result()

    for page_num, page in enumerate(form_recognizer_results.pages):
        tables_on_page = [
            table
            for table in form_recognizer_results.tables
            if table.bounding_regions[0].page_number == page_num + 1
        ]

        # mark all positions of the table spans in the page
        page_offset = page.spans[0].offset
        page_length = page.spans[0].length
        table_chars = [-1] * page_length
        for table_id, table in enumerate(tables_on_page):
            for span in table.spans:
                # replace all table spans with "table_id" in table_chars array
                for i in range(span.length):
                    idx = span.offset - page_offset + i
                    if idx >= 0 and idx < page_length:
                        table_chars[idx] = table_id

        # build page text by replacing characters in table spans with table html
        page_text = ""
        added_tables = set()
        for idx, table_id in enumerate(table_chars):
            if table_id == -1:
                page_text += form_recognizer_results.content[page_offset + idx]
            elif table_id not in added_tables:
                page_text += table_to_html(tables_on_page[table_id])
                added_tables.add(table_id)

        page_text += " "
        page_map.append((page_num, offset, page_text))
        offset += len(page_text)


    return page_map

# Convert text to Markdown
from markdownify import markdownify as md
def convert_to_markdown(text):
    return md(text)


def process_pdf(data: pd.DataFrame ):
    # Analyze document 
    pdf_content = get_document_text(filename=data["path"].iloc[0])
    print(data["path"].iloc[0])
    pdf_content2=[i[2] for i in pdf_content]

    # Convert to markdown
    md_text = convert_to_markdown("\n".join(pdf_content2))

    return pd.DataFrame([{"filename":data["path"].iloc[0],"content":md_text}])



# COMMAND ----------

list_pdfs = [ {"path":f[0].replace("dbfs:",""),"name":f[1],"size":f[2],"modificationTime":f[3]} for f in dbutils.fs.ls("/Volumes/main/default/myfiles/budget")]
list_pdfs

# COMMAND ----------

# creating a dataframe 
df_pdfs = spark.createDataFrame(list_pdfs) 
display(df_pdfs)

# COMMAND ----------

df_pdfs.write.mode("overwrite").parquet("/Volumes/main/default/myfiles/budget_results/df_pdfs")

# COMMAND ----------

sc.defaultParallelism

# COMMAND ----------

#Run time : 48min
from pyspark.sql.types import *

result_schema =StructType([
  StructField('filename',StringType()),
  StructField('content',StringType())
  ])

results = (
  df_pdfs
  .groupBy('path')
  .applyInPandas(process_pdf, schema=result_schema)
    )

# Save Results
results.write.mode("overwrite").parquet("/Volumes/main/default/myfiles/budget_results/results_pdf_to_md")


# COMMAND ----------

#Run time: 43min
from pyspark.sql.types import *

result_schema =StructType([
  StructField('filename',StringType()),
  StructField('content',StringType())
  ])

results = (
  df_pdfs
  .repartition(sc.defaultParallelism)
  .groupBy('path')
  .applyInPandas(process_pdf, schema=result_schema)
    )

# Save Results
results.write.mode("overwrite").parquet("/Volumes/main/default/myfiles/budget_results/results_pdf_to_md_repartition16")


# COMMAND ----------

display(spark.read.parquet("/Volumes/main/default/myfiles/budget_results/results_pdf_to_md_repartition16"))

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import pandas_udf
from typing import Iterator

@pandas_udf("data pd.DataFrame")
def process_pdf(data: pd.Series )-> pd.DataFrame:
    # Analyze document 
    pdf_content = get_document_text_local(filename=data.str)
    pdf_content2=[i[2] for i in pdf_content]

    # Convert to markdown
    md_text = convert_to_markdown("\n".join(pdf_content2))

    return pd.DataFrame([{"filename":data.str,"content":md_text}])


results_2 = df_pdfs.select(process_pdf("path"))
# Save Results
results_2.write.mode("overwrite").parquet("/Volumes/main/default/myfiles/budget_results/results2_pdf_to_md")



# COMMAND ----------

#display(results_2)

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install synapseml

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

document_path = "/Volumes/main/default/myfiles/budget/tm-mf-en_8.pdf"  # path to your document
df = spark.read.format("binaryFile").load(document_path).limit(10).cache()

# COMMAND ----------

display(df)

# COMMAND ----------

from synapse.ml.services.form import AnalyzeDocument
from pyspark.sql.functions import col

analyze_document = (
    AnalyzeDocument()
    .setPrebuiltModelId("prebuilt-layout")
    .setSubscriptionKey("f4769a30650545f2a6a9102a49693d58")
    .setLocation("eastus")
    .setImageBytesCol("content")
    .setOutputCol("result")
    .setPages(
        "1-15"
    )  # Here we are reading the first 15 pages of the documents for demo purposes
)

analyzed_df = (
    analyze_document.transform(df)
    .withColumn("output_content", col("result.analyzeResult.content"))
    .withColumn("paragraphs", col("result.analyzeResult.paragraphs"))
).cache()

# COMMAND ----------


