package io.github.agents;

import com.t4a.annotations.Action;
import com.t4a.annotations.Agent;
import com.t4a.predict.PredictionLoader;
import com.t4a.processor.AIProcessingException;
import com.t4a.processor.AIProcessor;
import jakarta.annotation.PostConstruct;
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
public class YogaService {

    private  AIProcessor processor;  // Add this field

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
        String response = getProcessor().query("Convert the following English query to a Cypher query :provide only cypher query and no other text " + englishQuery);
        String cipherQuery = response.replaceAll("```(?:cypher)?\\s*", "").trim();
        return new CypherResponse(cipherQuery);
    }
}
