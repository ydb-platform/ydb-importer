package tech.ydb.importer.integration;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author zinal
 */
public interface InputAny {

    static List<InputAny> getInputs() {
        List<InputAny> inputs = new ArrayList<>();
        inputs.add(new InputBasic());
        return inputs;
    }

    List<String> getCreate();

    List<String> getDrop();

    List<String> getInsert();

}
