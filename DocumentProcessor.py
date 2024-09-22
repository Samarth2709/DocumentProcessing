import os
import mimetypes
from unstructured.partition.auto import partition
from unstructured.chunking.title import chunk_by_title
from unstructured_client import UnstructuredClient
from unstructured_client.models import shared
from unstructured_client.models.errors import SDKError
from unstructured.staging.base import dict_to_elements


class DocumentProcessor:
    def __init__(self, file_path, unstructured_api_key=None, chunking=True, exists_tables=False):
        """
        Initializes the DocumentProcessor class.

        Args:
            file_path (str): Full file path of the document.
            unstructured_api_key (str, optional): API key for the Unstructured API. Defaults to None.
            chunking (bool, optional): Whether to chunk the document. Defaults to True.
            exists_tables (bool, optional): Whether the document contains tables. Defaults to False.
        """
        print(f"Initializing DocumentProcessor for file: {file_path}")
        self.file_path = file_path
        self.chunking = chunking
        self.exists_tables = exists_tables
        if unstructured_api_key is None:
            self.unstructured_api_key = os.getenv("SAMARTH_UNSTRUCTURED_API_KEY")
            if not self.unstructured_api_key:
                raise ValueError("Unstructured API key must be provided or set in environment variables.")
        else:
            self.unstructured_api_key = unstructured_api_key
        self.elements = None

    def get_file_type(self):
        """
        Determines the file type based on MIME type.

        Returns:
            str: The file type (e.g., 'pdf', 'docx', 'txt').
        """
        file_type, _ = mimetypes.guess_type(self.file_path)
        print(f"Determined MIME type: {file_type}")
        if file_type == 'application/pdf':
            return 'pdf'
        elif file_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
            return 'docx'
        elif file_type == 'text/plain':
            return 'txt'
        elif file_type == 'text/markdown':
            return 'md'
        else:
            return 'unknown'

    def preprocess(self):
        """
        Preprocesses the document using the appropriate method based on file type.

        Returns:
            list: A list of unstructured elements extracted from the document.
        """
        file_type = self.get_file_type()
        print(f"File type identified as: {file_type}")
        if file_type == 'pdf':
            self.elements = self.preprocess_pdf()
        else:
            self.elements = self.rule_partition()
        return self.elements

    def preprocess_pdf(self):
        """
        Preprocesses a PDF document using the Unstructured API.

        Returns:
            list: A list of unstructured elements extracted from the PDF.
        """
        print("Starting PDF preprocessing...")
        s = UnstructuredClient(
            api_key_auth=self.unstructured_api_key,
            server_url="https://api.unstructuredapp.io/general/v0/general",
        )
        with open(self.file_path, "rb") as f:
            files = shared.Files(
                content=f.read(),
                file_name=os.path.basename(self.file_path),
            )
        req = shared.PartitionParameters(
            files=files,
            strategy="hi_res",
            pdf_infer_table_structure=self.exists_tables,
            languages=["eng"],
            coordinates=True,
        )
        try:
            resp = s.general.partition(req)
            elements = dict_to_elements(resp.elements)
            print(f"Received {len(elements)} elements from the Unstructured API.")
            if self.chunking:
                print("Applying chunking to elements...")
                elements = chunk_by_title(
                    elements,
                    max_characters=1024,
                    new_after_n_chars=512,
                    include_orig_elements=True,
                    multipage_sections=True,
                )
                print(f"After chunking, we have {len(elements)} elements.")
            print("Example element:", elements)
            print("\nText: ", elements[0].text)
            print("\nText: ", elements[0].category)
            # {'type': 'Title', 'element_id': '7eb8a2636f10255abff6a602b57567ca', 'text': 'About Men', 'metadata': {'coordinates': {'points': ((42.0, 235.0), (42.0, 277.0), (381.0, 277.0), (381.0, 235.0)), 'system': 'PixelSpace', 'layout_width': 1700, 'layout_height': 2200}, 'filetype': 'application/pdf', 'languages': ['eng'], 'page_number': 1, 'filename': 'Elapsed Expectations Alan Lightman.pdf'}}
            return elements
        except SDKError as e:
            print(f"Error processing PDF: {e}")
            return None

    def rule_partition(self):
        """
        Preprocesses non-PDF documents using rule-based partitioning.

        Returns:
            list: A list of unstructured elements extracted from the document.
        """
        print("Starting rule-based partitioning...")
        try:
            elements = partition(self.file_path)
            print(f"Received {len(elements)} elements from partitioning.")
            if self.chunking:
                print("Applying chunking to elements...")
                elements = chunk_by_title(
                    elements,
                    max_characters=1024,
                    new_after_n_chars=512,
                    include_orig_elements=True,
                    multipage_sections=True,
                )
                print(f"After chunking, we have {len(elements)} elements.")
            return elements
        except Exception as e:
            print(f"Error partitioning file: {e}")
            return None

    def convert_to_markdown(self):
        """
        Converts the preprocessed elements into Markdown format.

        Returns:
            str: The document content in Markdown format.
        """
        if self.elements is None:
            print("No elements to convert. Please run preprocess() first.")
            return None

        markdown_text = ""
        print("Converting elements to Markdown...")
        for idx, element in enumerate(self.elements):
            print(f"Processing element {idx+1}/{len(self.elements)}: {type(element)}")
            markdown_text += self.element_to_markdown(element)
        return markdown_text

    def element_to_markdown(self, element):
        """
        Converts an element or composite element to Markdown format.

        Args:
            element: The element or composite element to convert.

        Returns:
            str: The Markdown representation of the element.
        """
        if hasattr(element, 'category'):
            # Handle single elements
            element_type = element.category
            text = element.text.strip() if element.text else ""

            if not text:
                return ""  # Skip elements without text

            print(f"Element type: {element_type}")
            if element_type == 'Title':
                return f"# {text}\n"
            elif element_type == 'Heading':
                return f"## {text}\n"
            elif element_type == 'Subheading':
                return f"### {text}\n"
            elif element_type == 'UnorderedList':
                markdown = ""
                for item in text.split('\n'):
                    markdown += f"- {item.strip()}\n"
                return markdown + "\n"
            elif element_type == 'OrderedList':
                markdown = ""
                for idx, item in enumerate(text.split('\n'), 1):
                    markdown += f"{idx}. {item.strip()}\n"
                return markdown + "\n"
            elif element_type == 'Table':
                return self.table_to_markdown(element)
            elif element_type == 'Figure':
                caption = element.metadata.get('caption', 'Figure')
                return f"![{caption}]({self.file_path})\n\n"
            else:
                return f"{text}\n\n"
        elif hasattr(element, 'elements'):
            # Handle CompositeElement
            print("Processing CompositeElement...")
            markdown = ""
            for sub_element in element.elements:
                markdown += self.element_to_markdown(sub_element)
            return markdown
        else:
            print("Unknown element type:\n", element, element.text)
            return ""

    def table_to_markdown(self, element):
        """
        Converts a table element into Markdown table format.

        Args:
            element: The table element containing data.

        Returns:
            str: The table in Markdown format.
        """
        # Placeholder implementation for table conversion
        markdown_table = "[Table content not available]\n\n"
        return markdown_table

    def save_markdown(self, output_path, markdown_text=None):
        """
        Saves the converted Markdown text to a file.

        Args:
            output_path (str): The file path where the Markdown content will be saved.
        """
        if markdown_text is None:
            markdown_text = self.convert_to_markdown()
        if markdown_text:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(markdown_text)
            print(f"Markdown content saved to {output_path}")
        else:
            print("No Markdown content to save.")


# Example usage:
if __name__ == "__main__":
    # Replace with the path to your document
    file_path = '/Users/samarthkumbla/Downloads/Elapsed Expectations Alan Lightman.pdf'
    processor = DocumentProcessor(file_path, chunking=False)
    processor.preprocess()
    markdown_content = processor.convert_to_markdown()
    # print(markdown_content)
    # Optionally, save the markdown content to a file
    if markdown_content:
        output_markdown_file = 'output_document.md'
        processor.save_markdown(output_markdown_file, markdown_text=markdown_content)
