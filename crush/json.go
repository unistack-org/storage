package crush

import "encoding/json"

func (cmap *Wrapper) DecodeJson(data []byte) error {
	err := json.Unmarshal(data, &cmap)
	if err != nil {
		return err
	}
	cmap.rulesSort()
	cmap.bucketsSort()
	return nil
}

func (cmap *Wrapper) EncodeJson() ([]byte, error) {
	return json.Marshal(cmap)
}
