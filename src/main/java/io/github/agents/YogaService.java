package io.github.agents;

import com.t4a.annotations.Action;
import com.t4a.annotations.Agent;
import com.t4a.predict.PredictionLoader;
import com.t4a.processor.AIProcessingException;
import com.t4a.processor.AIProcessor;
import io.github.vishalmysore.SchemaExtractor;
import jakarta.annotation.PostConstruct;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Service
@RestController
@RequestMapping("/api/yoga")
@CrossOrigin(origins = "*")
@Agent(groupName = "yogaServices", groupDescription = "all yoga related services and information")
@Log
public class YogaService {

    private  AIProcessor processor;  // Add this field

    private String yogaSchemaInfo;
    @Value("${yoga.db.path:db/yoga.kuzu}")
    private String yogaDbPath;
    @PostConstruct
    public void init() {
        // Load or initialize the yoga knowledge graph schema information
        yogaSchemaInfo = SchemaExtractor.getSchemaForDB(yogaDbPath, "Yoga");
        log.info(yogaSchemaInfo);
    }

    //this is needed if you want to initialize the processor once the service is constructed
    //only needed whwne you call thru spring controller
    private AIProcessor getProcessor() {
        if(processor == null) {
            processor = PredictionLoader.getInstance().createOrGetAIProcessor();
        }
        return processor;
    }
    @RequestMapping("/info")
    @Action
    public String getYogaInfo() {
        return "Welcome to the Yoga Knowledge Service!";
    }

    @RequestMapping("/benefits")
    @Action
    public String getYogaBenefits() throws AIProcessingException {
        // Now you can use the processor to query
        String response = getProcessor().query("What are the benefits of yoga?");
        return "Welcome to the Yoga Knowledge Service! " + response;
    }

    @RequestMapping("/pose/{poseName}")
    @Action
    public String getPoseInfo(@PathVariable String poseName) throws AIProcessingException {
        String response = getProcessor().query("Tell me about the " + poseName + " yoga pose");
        return response;
    }

    @RequestMapping("/graph/{englishQuery}")
    @Action
    public CypherResponse convertToCipherQuery(@PathVariable String englishQuery) throws AIProcessingException {
        String prommpt = "Convert the following English query to a Cypher query :provide only cypher query and no other text " + englishQuery+" here is the schema info "+yogaSchemaInfo;
        log.info(prommpt);
        String response = getProcessor().query(prommpt);
        String cipherQuery = response.replaceAll("```(?:cypher)?\\s*", "").trim();
        return new CypherResponse(cipherQuery);
    }
}