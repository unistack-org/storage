package crushmap

import "encoding/json"

func (cmap *Map) DecodeJson(data []byte) error {
	err := json.Unmarshal(data, &cmap)
	if err != nil {
		return err
	}
	cmap.rulesSort()
	cmap.bucketsSort()
	return nil
}

func (cmap *Map) EncodeJson() ([]byte, error) {
	return json.Marshal(cmap)
}
