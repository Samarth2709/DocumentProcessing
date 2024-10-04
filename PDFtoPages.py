from pdf2image import convert_from_path
import os
from tqdm import tqdm
from pdf2image.exceptions import PDFPageCountError, PDFSyntaxError

class PDFToImagesConverter:
    def __init__(self, pdf_path, output_folder=None, dpi=300):
        self.pdf_path = pdf_path
        # Set default output folder to be the file title + "_output_images"
        base_filename = os.path.splitext(os.path.basename(pdf_path))[0]
        self.output_folder = output_folder or f'{base_filename}_output_images'
        self.dpi = dpi  # DPI value to control the quality of the output images

    def convert(self):
        # Create output folder if it doesn't exist
        os.makedirs(self.output_folder, exist_ok=True)  # Create the directory where images will be saved

        # Convert PDF to images
        try:
            # Convert PDF pages to images using the specified DPI (dots per inch) for quality
            pages = convert_from_path(self.pdf_path, self.dpi)
            # Use tqdm to add progress tracking
            for i, page in enumerate(tqdm(pages, desc="Converting pages", unit="page")):
                # Extract the original filename without extension
                base_filename = os.path.splitext(os.path.basename(self.pdf_path))[0]
                # Create the file path for each image (naming them with original filename plus page number)
                image_path = os.path.join(self.output_folder, f'{base_filename}_pg{i + 1}.jpg')
                # Save the page as a JPEG image
                page.save(image_path, 'JPEG')
                # Print a message indicating the image has been saved successfully
                print(f'Saved: {image_path}')
        except FileNotFoundError:
            print("Error: The specified PDF file was not found.")
        except PDFPageCountError:
            print("Error: Unable to count the pages in the PDF. The file may be corrupted or not a valid PDF.")
        except PDFSyntaxError:
            print("Error: The PDF syntax is invalid. The file may be corrupted.")
        except Exception as e:
            # Print an error message if something goes wrong during the conversion process
            print(f"Unexpected error: {e}")

def main():
    # Get the path to the PDF file from the user
    pdf_path = input("Enter the path to the PDF file: ")
    # Get the output folder name from the user, or use the default based on the file name
    output_folder = input("Enter the output folder name (default: <filename>_output_images): ") or None
    # Get the DPI value from the user, or use the default 300
    dpi = int(input("Enter the DPI value for image quality (default: 300): ") or 300)
    # Create an instance of PDFToImagesConverter and convert the PDF to images
    converter = PDFToImagesConverter(pdf_path, output_folder, dpi)
    converter.convert()

if __name__ == "__main__":
    main()  # Run the main function when the script is executed