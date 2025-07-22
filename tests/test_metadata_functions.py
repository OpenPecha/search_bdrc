import pytest
from unittest.mock import patch, Mock
from pathlib import Path
import os
from rdflib import Graph, Namespace, URIRef, Literal, RDF, RDFS
from search_bdrc import BdrcScraper

@pytest.fixture
def mock_metadata_graph():
    g = Graph()
    BDO = Namespace("http://purl.bdrc.io/ontology/core/")
    g.bind('bdo', BDO)
    
    # Add test triples
    test_outline = URIRef("http://purl.bdrc.io/resource/O1234")
    test_subject = URIRef("http://purl.bdrc.io/resource/test_subject")
    g.add((test_subject, BDO.hasOutline, test_outline))
    
    return g

@pytest.fixture
def scraper():
    return BdrcScraper()

class TestMetadataFunctions:
    def test_get_instance_metadata_success(self, scraper):
        with patch('requests.get') as mock_get:
            # Mock successful response
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.text = """
            @prefix bdo: <http://purl.bdrc.io/ontology/core/> .
            @prefix test: <http://purl.bdrc.io/resource/test_subject> .
            test: bdo:hasOutline <http://purl.bdrc.io/resource/O1234> .
            """
            mock_get.return_value = mock_response
            
            result = scraper.get_instance_metadata("TEST123")
            
            assert isinstance(result, Graph)
            mock_get.assert_called_once_with(
                "https://ldspdi-dev.bdrc.io/resource/TEST123.ttl",
                headers={"Accept": "text/turtle"}
            )

    def test_get_instance_metadata_failure(self, scraper):
        with patch('requests.get') as mock_get:
            # Mock failed response
            mock_response = Mock()
            mock_response.status_code = 404
            mock_get.return_value = mock_response
            
            result = scraper.get_instance_metadata("TEST123")
            
            assert result is None
            mock_get.assert_called_once()

    def test_get_instance_metadata_json_success(self, scraper):
        with patch('requests.get') as mock_get:
            # Mock successful JSON response
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"test": "data"}
            mock_get.return_value = mock_response
            
            result = scraper.get_instance_metadata("TEST123", json_format=True)
            
            assert result == {"test": "data"}
            mock_get.assert_called_once_with(
                "https://purl.bdrc.io/resource/TEST123.jsonld",
                headers={"Accept": "application/ld+json"}
            )

    def test_get_outline_of_instance_success(self, scraper, mock_metadata_graph, tmp_path):
        with patch.object(scraper, 'get_instance_metadata') as mock_get_metadata:
            mock_get_metadata.return_value = mock_metadata_graph
            
            # Set up temporary output directory
            original_cwd = os.getcwd()
            try:
                Path(tmp_path).mkdir(exist_ok=True)
                os.chdir(str(tmp_path))
                
                result = scraper.get_outline_of_instance("TEST123")
                
                assert isinstance(result, list)
                assert len(result) == 1
                assert result[0] == "O1234"
                
                # Verify metadata file was created
                assert (Path("outputs") / "TEST123_metadata.ttl").exists()
                
            finally:
                os.chdir(original_cwd)

    def test_get_outline_of_instance_no_metadata(self, scraper):
        with patch.object(scraper, 'get_instance_metadata') as mock_get_metadata:
            mock_get_metadata.return_value = None
            
            result = scraper.get_outline_of_instance("TEST123")
            
            assert isinstance(result, list)
            assert len(result) == 0

    def test_get_outline_of_instance_no_outlines(self, scraper):
        with patch.object(scraper, 'get_instance_metadata') as mock_get_metadata:
            # Create graph with no outlines
            g = Graph()
            mock_get_metadata.return_value = g
            
            result = scraper.get_outline_of_instance("TEST123")
            
            assert isinstance(result, list)
            assert len(result) == 0

    def test_get_outline_metadata(self, scraper, mock_metadata_graph):
        with patch.object(scraper, 'get_instance_metadata') as mock_get_metadata:
            mock_get_metadata.return_value = mock_metadata_graph
            
            result = scraper.get_outline_metadata("O1234")
            
            assert result == mock_metadata_graph
            mock_get_metadata.assert_called_once_with("O1234")

    def test_get_page_title_with_title(self, scraper):
        # Create a test graph with a title page title
        g = Graph()
        BDO = Namespace("http://purl.bdrc.io/ontology/core/")
        RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
        
        # Create a title page title node
        title_node = URIRef("http://purl.bdrc.io/resource/TITLE123")
        g.add((title_node, RDF.type, BDO.TitlePageTitle))
        g.add((title_node, RDFS.label, Literal("Test Title Page")))
        
        result = scraper.get_page_title(g)
        
        assert result == "Test Title Page"

    def test_get_page_title_no_title(self, scraper):
        # Create an empty graph
        g = Graph()
        
        result = scraper.get_page_title(g)
        
        assert result is None

    def test_get_page_title_multiple_titles(self, scraper):
        # Create a test graph with multiple title page titles
        g = Graph()
        BDO = Namespace("http://purl.bdrc.io/ontology/core/")
        RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
        
        # Create multiple title page title nodes
        title_node1 = URIRef("http://purl.bdrc.io/resource/TITLE123")
        title_node2 = URIRef("http://purl.bdrc.io/resource/TITLE456")
        
        g.add((title_node1, RDF.type, BDO.TitlePageTitle))
        g.add((title_node1, RDFS.label, Literal("First Title")))
        g.add((title_node2, RDF.type, BDO.TitlePageTitle))
        g.add((title_node2, RDFS.label, Literal("Second Title")))
        
        result = scraper.get_page_title(g)
        
        # Should return the first title found
        assert result in ["First Title", "Second Title"]

    def test_get_page_title_no_label(self, scraper):
        # Create a test graph with a title page title but no label
        g = Graph()
        BDO = Namespace("http://purl.bdrc.io/ontology/core/")
        
        title_node = URIRef("http://purl.bdrc.io/resource/TITLE123")
        g.add((title_node, RDF.type, BDO.TitlePageTitle))
        
        result = scraper.get_page_title(g)
        
        assert result is None
