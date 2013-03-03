package matrix;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapred.OutputCollector;

/**
 * A mock OutputCollector generic class for unit testing.
 * It simply saves the key/value pairs passed via collect() method into an ArrayList object,
 * so that testing codes can verify them later.
 * 
 * @author daybreaker
 *
 * @param <K>
 * @param <V>
 */
public class OutputCollectorMock<K, V> implements OutputCollector<K, V> {
	
	public ArrayList<K> keys = new ArrayList<K>();
	public ArrayList<V> values = new ArrayList<V>();

	@Override
	public void collect(K key, V value) throws IOException {
		keys.add(key);
		values.add(value);
	}
}

