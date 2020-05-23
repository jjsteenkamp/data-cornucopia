package uk.co.devworx.pdf;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class PdfMergeTest
{
	@Test
	public void testDocumentMergingDir() throws Exception
	{
		Path output = Paths.get("target/test-page-01-02-dir.pdf");
		Path dir = Paths.get("src/test/resources");
		PdfMerge.MergePDFFilesInDirectory(output, dir);
		PDDocument document = PDDocument.load(output.toFile());
		Assertions.assertEquals(2, document.getNumberOfPages());
		document.close();
	}

	@Test
	public void testDocumentMerging() throws Exception
	{
		Path output = Paths.get("target/test-page-01-02.pdf");
		Path p1 = Paths.get("src/test/resources/test-page-01.pdf");
		Path p2 = Paths.get("src/test/resources/test-page-02.pdf");

		PdfMerge.MergePDFFiles(output, p1, p2);

		PDDocument document = PDDocument.load(output.toFile());

		Assertions.assertEquals(2, document.getNumberOfPages());

		document.close();
	}

}
