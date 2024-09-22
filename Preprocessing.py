import os
import mimetypes
import gradio as gr

from unstructured.partition.auto import partition
from unstructured.chunking.title import chunk_by_title

# from unstructured.partition import partition_markdown

from unstructured_client import UnstructuredClient # Ustructured API Client
from unstructured_client.models import shared
from unstructured_client.models.errors import SDKError
from unstructured.staging.base import dict_to_elements # Converting API response to Unstructred Elements

import pandas as pd



class Preprocessing:
    def __init__(self, doc_folder = None, docs = None, path = None, chunking = True, exists_tables=False):
        """
        Args:
            doc_folder (str, optional): Folder with set of documents to preprocess. Defaults to None.
            docs (list[str], optional): List of document basenames. Defaults to None.
            path (str, optional): Path to load documents from. Defaults to None.
        """
        self.unstructured_api_key = os.getenv("SAMARTH_UNSTRUCTURED_API_KEY") # TODO - Add API Key to env

        if doc_folder is not None:
            self.doc_folder = doc_folder
            self.docs_basename = os.listdir(doc_folder)
            self.docs = [os.path.join(doc_folder, doc) for doc in self.docs_basename]
        elif docs is not None and path is not None:
            self.path = path
            self.docs_basename = docs
            self.docs = [os.path.join(path, doc) for doc in self.docs_basename]
        elif path is not None:
            self.path = path
            self.docs_basename = os.listdir(path)
            self.docs = [os.path.join(path, doc) for doc in self.docs_basename]
        else:
            raise ValueError("Either doc_folder or docs and path must be provided.")
        
        self.exists_tables = exists_tables
        self.chunking = chunking

        
        self.preprocessed_outputs = {}
        
    def get_preproceed_outputs(self):
        if self.preprocessed_outputs == {}:
            self.preprocess_files()
        return self.preprocessed_outputs
    
    def get_file_type(self, file_path):
        """
        Determine the type of a file.
        
        Parameters:
        file_path (str): The path to the file.
    
        Returns:
        str: The type of the file (e.g., 'pdf', 'xlsx', 'csv', etc.).
        """
        # Get the MIME type of the file
        file_type, _ = mimetypes.guess_type(file_path)
        
        # Map specific MIME types to desired file type strings
        if file_type == 'application/pdf':
            return 'pdf'
        elif file_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            return 'xlsx'
        elif file_type == 'application/vnd.ms-excel':
            return 'xlsx'
        elif file_type == 'text/csv':
            return 'csv'
        elif file_type == 'application/zip':
            # Handle older Excel file formats
            _, ext = os.path.splitext(file_path)
            if ext == '.xls':
                return 'xlsx'
            return 'zip'
        else:
            return 'Unknown file type'
    

    def preprocess_files(self, file_names=None):
        """
        Prerpocesses files to JSON

        Parameters:
        files (list): List of files paths to be preprocessed

        Returns:
        dict(file_name: UnstructuredElements): returns dict with the filename as the key and the preprocessed documentElements of the respective file
        """
        if file_names is None:
            file_names = self.docs
        
        print("Preprocessing Files...")
        # self.preprocessed_outputs  --  Structure is -> {file_name: documentElements}
        for file_path in file_names:
            file_name = os.path.basename(file_path)
            print(f"Preprocessing {file_name}...")

            # If file is a pdf file
            if self.get_file_type(file_name) == "pdf":
                self.preprocessed_outputs[file_name] = self.preprocess_pdf(file_path)

            else:
                self.preprocessed_outputs[file_name] = self.rule_partition(file_path)
        return self.preprocessed_outputs
    

    def preprocess_pdf(self, filepath, chunking=None):
        """
        Prerpocesses pdf to Unstructured Elements

        Parameters:
        filepath (str): Filepath of pdf file
        chunking (str): Optional either "by_title", "basic" or None.
                    Determines if chunking is applied to the JSON.
                    Defaults to None.

        Returns:
        list: Unstructured elements of document
        """
    
        # Create API Client
        s = UnstructuredClient(
            api_key_auth=self.unstructured_api_key,
            server_url="https://api.unstructuredapp.io/general/v0/general",
        )

        with open(filepath, "rb") as f:
            files=shared.Files(
            content=f.read(), 
            file_name=filepath,
            )

        if self.chunking:
            # Parameters for partitioning pdf
            req = shared.PartitionParameters(
                files=files, 
                strategy="hi_res", 
                pdf_infer_table_structure=self.exists_tables,
                languages=["eng"],
                coordinates=True,
            )
        else:
            # Parameters for partitioning pdf without chunking
            req = shared.PartitionParameters(
                files=files, 
                strategy="hi_res", 
                pdf_infer_table_structure=self.exists_tables,
                languages=["eng"], 

                coordinates=True,
            )

        try:
            resp = s.general.partition(req)
            # print(JSON(json.dumps(resp.elements, indent=2)))
            elements = dict_to_elements(resp.elements)
            if self.chunking:
                elements = chunk_by_title(elements,
                                            max_characters=1024,
                                            new_after_n_chars=512,
                                            include_orig_elements=True,
                                            multipage_sections=True,
                                            )
                return elements
            # return resp.elements
        except SDKError as e:
            print(e)
        
    def rule_partition(self, filepath):
        """
        Rule based partitioning

        Parameters:
        filepath (str): Filepath of file

        Returns:
        list: Unstructured elements of document
        """
        elements = partition(filepath) # Partitioning the file
        if self.chunking:
            # Chunk file if chunking is True
            elements = chunk_by_title(elements, 
                                      max_characters=1024,
                                      new_after_n_chars=512,
                                      include_orig_elements=True,
                                      multipage_sections=True,
                                      )
        
        return elements
    
    def filter_elements(self, element_types=[], elements = None):
        if elements is None:
            elements = self.preprocessed_outputs
        if element_types == []:
            return self.preprocessed_outputs
        
    
    def show_preprocessed_outputs(self):
        for file_name, elements in self.preprocessed_outputs.items():
            print(f"File: {file_name}\n")
            for i, element in enumerate(elements):
                print(str(i)+".", element)
            print("-"*10 + "\n")
    
    def save_preprocessed_outputs(self, output_folder="elements"):
        for file_name, elements in self.preprocessed_outputs.items():
            with open(os.path.join(output_folder, "Elements_"+os.path.basename(file_name))+".txt", "w") as f:
                for i, element in enumerate(elements):
                    f.write(f"{i}. " + str(element) + "\n\n")

        for file_name, elements in self.preprocessed_outputs.items():
            data = []
            for i, element in enumerate(elements):
                print(element)
                print(type(element))
                row = {}
                # row["Type"]  = element["type"]
                # row["ID"] = element["element_id"]
                # row["Text"] = element["text"]
                # row["Page"] = element["metadata"]["page_number"]
                row["Type"]  = element.type
                # row["ID"] = element.element_id"]
                row["Text"] = element.text
                row["Page"] = element.metadata.page_number
                data.append(row)
            df = pd.DataFrame(data)
            df.to_csv(os.path.join(output_folder, "Elements_"+os.path.basename(file_name))+".csv", index=False)


    
if __name__ == "__main__":
    folder_path = "/Users/samarthkumbla/Documents/Samarth/PythonProjects/RAG_pipeline/docs"
    filename = "Artificial Intelligence-Machine Learning Explained.pdf"
    p = Preprocessing(path=folder_path, docs=[filename])
    p.preprocess_files()
    p.show_preprocessed_outputs()
    p.save_preprocessed_outputs()