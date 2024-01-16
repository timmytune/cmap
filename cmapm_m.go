package cmap

import (
	"errors"
	"sync"
)

type ValueM struct {
	Keys   map[string]map[string]struct{}
	Values map[string]interface{}
	sync.RWMutex
}

type FieldValueM struct {
	Data map[*ValueM]struct{}

	sync.RWMutex
}

type FieldM struct {
	data map[string]*FieldValueM
	sync.RWMutex
}

type AccesserM struct {
	c      *CmapM
	V      *ValueM
	Fv     *FieldValueM
	Filter map[*ValueM]struct{}
	err    error
}

func (a *AccesserM) Set(field string, key string) (ac *AccesserM) {
	ac = a

	if a.err != nil {
		return
	}

	if a.V == nil {
		a.err = errors.New("Accesser.Set must have value set ")
		return
	}

	if field == "" {
		ac.err = errors.New("empty field provided in function Accesser.Set")
		return
	}

	if key == "" {
		ac.err = errors.New("empty key provided in function Accesser.Set")
		return
	}

	a.c.RLock()
	f, ok := a.c.data[field]
	a.c.RUnlock()

	if !ok {

		f := FieldM{}
		f.data = make(map[string]*FieldValueM)

		fv := FieldValueM{}
		fv.Data = make(map[*ValueM]struct{}, 0)

		//setting
		a.V.Lock()
		if a.V.Keys[field] == nil {
			a.V.Keys[field] = make(map[string]struct{})
		}
		a.V.Keys[field][key] = struct{}{}
		a.V.Unlock()

		fv.Data[a.V] = struct{}{}

		f.data[key] = &fv

		a.c.Lock()
		a.c.data[field] = &f
		a.c.Unlock()

		a.Fv = &fv

		return
	}

	f.RLock()
	fv, ok := f.data[key]
	f.RUnlock()

	if !ok {
		fv := FieldValueM{}
		fv.Data = make(map[*ValueM]struct{})

		//setting
		a.V.Lock()
		if a.V.Keys[field] == nil {
			a.V.Keys[field] = make(map[string]struct{})
		}
		a.V.Keys[field][key] = struct{}{}
		a.V.Unlock()

		fv.Data[a.V] = struct{}{}

		f.Lock()
		f.data[key] = &fv
		f.Unlock()

		a.Fv = &fv

		return
	}

	//vInFv := false

	fv.Lock()
	// for _, v1 := range fv.Data {
	// 	if v1 == a.V {
	// 		vInFv = true
	// 		break
	// 	}
	// }

	_, ok = fv.Data[a.V]
	if !ok {

		a.V.Lock()
		if a.V.Keys[field] == nil {
			a.V.Keys[field] = make(map[string]struct{})
		}
		a.V.Keys[field][key] = struct{}{}
		a.V.Unlock()

		fv.Data[a.V] = struct{}{}
	}
	fv.Unlock()

	a.Fv = fv

	return
}

func (a *AccesserM) FilterKV(f map[string]string) (ac *AccesserM) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make(map[*ValueM]struct{})

	if len(a.Filter) > 0 {
		for v := range a.Filter {

			pass := true
			v.RLock()
			for kf, vf := range f {
				pass2 := false
				for vk := range v.Keys[kf] {
					if vf == vk {
						pass2 = true
					}
				}
				if !pass2 {
					pass = false
				}
			}
			v.RUnlock()

			if pass {
				filter[v] = struct{}{}
			}
		}
	} else {
		a.Fv.RLock()
		for v := range a.Fv.Data {

			pass := true
			v.RLock()
			for kf, vf := range f {
				pass2 := false
				for vk := range v.Keys[kf] {
					if vf == vk {
						pass2 = true
					}
				}
				if !pass2 {
					pass = false
				}
			}
			v.RUnlock()

			if pass {
				filter[v] = struct{}{}
			}
		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		for k := range filter {
			a.V = k
		}
		a.Filter = filter
	}

	return
}

func (a *AccesserM) FilterNoKey(key string) (ac *AccesserM) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make(map[*ValueM]struct{}, 0)

	if len(a.Filter) > 0 {
		for v := range a.Filter {

			v.RLock()
			_, ok := v.Keys[key]
			v.RUnlock()
			if !ok {
				filter[v] = struct{}{}
			}

		}
	} else {
		a.Fv.RLock()
		for v := range a.Fv.Data {
			v.RLock()
			_, ok := v.Keys[key]
			v.RUnlock()
			if !ok {
				filter[v] = struct{}{}
			}
		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		for k := range filter {
			a.V = k
		}
		a.Filter = filter
	}

	return
}

func (a *AccesserM) FilterHasKey(key string) (ac *AccesserM) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make(map[*ValueM]struct{})

	if len(a.Filter) > 0 {
		for v := range a.Filter {

			v.RLock()
			_, ok := v.Keys[key]
			v.RUnlock()
			if ok {
				filter[v] = struct{}{}
			}

		}
	} else {
		a.Fv.RLock()
		for v := range a.Fv.Data {
			v.RLock()
			_, ok := v.Keys[key]
			v.RUnlock()
			if ok {
				filter[v] = struct{}{}
			}
		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		for k := range filter {
			a.V = k
		}
		a.Filter = filter
	}

	return
}

func (a *AccesserM) FilterHasValue(key string) (ac *AccesserM) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make(map[*ValueM]struct{})

	if len(a.Filter) > 0 {
		for v := range a.Filter {

			v.Lock()
			if v.Values == nil {
				v.Values = make(map[string]interface{})
			}
			_, ok := v.Values[key]
			v.Unlock()
			if ok {
				filter[v] = struct{}{}
			}

		}
	} else {
		a.Fv.RLock()
		for v := range a.Fv.Data {

			v.Lock()
			if v.Values == nil {
				v.Values = make(map[string]interface{})
			}
			_, ok := v.Values[key]
			v.Unlock()
			if ok {
				filter[v] = struct{}{}
			}

		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		for k := range filter {
			a.V = k
		}
		a.Filter = filter
	}

	return
}

func (a *AccesserM) FilterNotValue(key string) (ac *AccesserM) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make(map[*ValueM]struct{})
	if len(a.Filter) > 0 {
		for v := range a.Filter {

			v.Lock()
			if v.Values == nil {
				v.Values = make(map[string]interface{})
			}
			_, ok := v.Values[key]
			v.Unlock()
			if !ok {
				filter[v] = struct{}{}
			}

		}
	} else {
		a.Fv.RLock()
		for v := range a.Fv.Data {

			v.Lock()
			if v.Values == nil {
				v.Values = make(map[string]interface{})
			}
			_, ok := v.Values[key]
			v.Unlock()
			if !ok {
				filter[v] = struct{}{}
			}

		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		for k := range filter {
			a.V = k
		}
		a.Filter = filter
	}

	return
}

func (a *AccesserM) SetValue(key string, val interface{}) (ac *AccesserM) {
	ac = a

	if a.err != nil {
		return
	}

	if key == "" {
		a.err = errors.New("empty key provided in function Accesser.SetValue")
		return
	}

	if a.V == nil {
		a.err = errors.New("acceser has a nil value")
		return
	}

	a.V.Lock()
	if a.V.Values == nil {
		a.V.Values = make(map[string]interface{})
	}
	a.V.Values[key] = val
	a.V.Unlock()

	return
}

func (a *AccesserM) GetValue(key string) (val interface{}, err error) {

	if a.err != nil {
		err = a.err
		return
	}

	if key == "" {
		a.err = errors.New("empty key provided in function Accesser.GetValue")
		return
	}

	if a.V == nil {
		err = errors.New("acceser has a nil value")
		return
	}

	ok := false

	a.V.RLock()
	if a.V.Values == nil {
		a.V.Values = make(map[string]interface{})
	}
	val, ok = a.V.Values[key]
	a.V.RUnlock()

	if !ok {
		a.err = errors.New("V does not have the value" + key + "set")
		err = a.err
	}

	return
}

func (a *AccesserM) GetValueString(key string) (vaa string, err error) {

	if a.err != nil {
		err = a.err
		return
	}

	if a.V == nil {
		err = errors.New("acceser has a nil value")
		return
	}

	a.V.RLock()
	if a.V.Values == nil {
		a.V.Values = make(map[string]interface{})
	}
	val, ok := a.V.Values[key]
	a.V.RUnlock()

	if !ok {
		return "", nil
	}

	vaa, ok = val.(string)
	if !ok {
		return "", errors.New("data in interface not string")
	}

	return
}

func (a *AccesserM) GetField(key string) (val map[string]struct{}, err error) {

	if a.err != nil {
		err = a.err
		return
	}

	if a.V == nil {
		err = errors.New("acceser has a nil value")
		return
	}

	ok := false

	a.V.RLock()
	if a.V.Keys == nil {
		a.V.Keys = make(map[string]map[string]struct{})
	}
	val, ok = a.V.Keys[key]
	a.V.RUnlock()

	if !ok {
		a.err = errors.New("value does not have the key '" + key + "' set")
		err = a.err
	}

	return
}

func (a *AccesserM) DeleteValue(key string) (err error) {

	if a.err != nil {
		err = a.err
		return
	}

	if a.V == nil {
		err = errors.New("acceser has a nil value")
		return
	}

	a.V.Lock()
	if a.V.Values == nil {
		a.V.Values = make(map[string]interface{})
	}
	delete(a.V.Values, key)
	a.V.Unlock()

	return
}

func (a *AccesserM) DeleteField(field string) (err error) {
	if field == "" {
		a.err = errors.New("field cannot be empty")
		err = a.err
		return
	}

	if a.err != nil {
		err = a.err
		return
	}

	if a.V == nil {
		err = errors.New("acceser has a nil value")
		return
	}

	a.V.Lock()
	key, ok := a.V.Keys[field]
	delete(a.V.Keys, field)
	a.V.Unlock()

	if !ok {
		return
	}

	a.c.RLock()
	f, ok := a.c.data[field]
	a.c.RUnlock()

	if !ok {
		return
	}

	fvs := make(map[string]*FieldValueM)

	f.RLock()
	for k := range key {
		fv, ok := f.data[k]
		if ok {
			fvs[k] = fv
		}
	}
	f.RUnlock()

	if len(fvs) == 0 {
		return
	}

	for k, v := range fvs {

		//del := false

		v.Lock()

		delete(v.Data, a.V)

		fvlen := len(v.Data)

		// if fvlen == 1 && v.Data[0] == a.V {
		// 	del = true
		// } else if fvlen == 1 && v.Data[0] != a.V {
		// 	a.err = errors.New("fieldvalue with this key does not have the current value attached ")
		// 	err = a.err
		// } else if fvlen > 1 {
		// 	for k2, v2 := range v.Data {
		// 		if v2 == a.V {
		// 			if k2 != len(v.Data)-1 {
		// 				v.Data[k2] = v.Data[len(v.Data)-1]
		// 			}
		// 			v.Data = v.Data[:len(v.Data)-1]
		// 			break
		// 		}
		// 	}
		// } else if fvlen == 0 {
		// 	del = true
		// }

		v.Unlock()

		if fvlen == 0 {
			f.Lock()
			delete(f.data, k)
			f.Unlock()
		}

	}

	return
}

func (a *AccesserM) DeleteFieldValue(field string, value string) (err error) {
	if field == "" {
		a.err = errors.New("field cannot be empty")
		err = a.err
		return
	}

	if a.err != nil {
		err = a.err
		return
	}

	if a.V == nil {
		err = errors.New("acceser has a nil value")
		return
	}

	a.V.Lock()
	ok2 := false
	key, ok := a.V.Keys[field]
	if ok {
		_, ok2 = key[value]
		if ok2 {
			delete(a.V.Keys[field], value)
		}
	}
	a.V.Unlock()
	if !ok2 {
		return
	}

	a.c.RLock()
	f, ok := a.c.data[field]
	a.c.RUnlock()
	if !ok {
		return
	}

	var v *FieldValueM

	f.RLock()
	v, ok = f.data[value]
	f.RUnlock()

	if !ok {
		return
	}

	//del := false

	v.Lock()

	delete(v.Data, a.V)

	// if fvlen == 1 && v.Data[0] == a.V {
	// 	del = true
	// } else if fvlen == 1 && v.Data[0] != a.V {
	// 	a.err = errors.New("fieldvalue with this key does not have the current value attached ")
	// 	err = a.err
	// } else if fvlen > 1 {
	// 	for k2, v2 := range v.Data {
	// 		if v2 == a.V {
	// 			if k2 != len(v.Data)-1 {
	// 				v.Data[k2] = v.Data[len(v.Data)-1]
	// 			}
	// 			v.Data = v.Data[:len(v.Data)-1]
	// 			break
	// 		}
	// 	}
	// } else if fvlen == 0 {
	// 	del = true
	// }

	v.Unlock()

	if len(v.Data) == 0 {
		f.Lock()
		delete(f.data, value)
		f.Unlock()
	}

	return
}

func (a *AccesserM) Delete() (err error) {

	if a.err != nil {
		err = a.err
		return
	}

	if a.V == nil {
		err = errors.New("acceser has a nil value")
		return
	}

	a.V.Lock()
	keys := a.V.Keys
	a.V.Unlock()

	for field, key := range keys {

		a.c.RLock()
		f, ok := a.c.data[field]
		a.c.RUnlock()

		if !ok {
			a.err = errors.New("field" + field + " does not exist on parent map")
			err = a.err
		} else {

			for keyValue := range key {

				f.RLock()
				fv, ok := f.data[keyValue]
				f.RUnlock()

				if !ok {
					a.err = errors.New("fieldvalue with this key" + keyValue + " does not exist on field " + field)
					err = a.err
				} else {

					//delFIeldVal := false

					fv.Lock()
					delete(fv.Data, a.V)
					fvlen := len(fv.Data)

					// if fvlen == 1 && fv.Data[0] == a.V {
					// 	delFIeldVal = true
					// } else if fvlen == 1 && fv.Data[0] != a.V {
					// 	a.err = errors.New("fieldvalue with this key" + keyValue + " does not have the current value attached ")
					// 	err = a.err
					// } else if fvlen > 1 {
					// 	for k, v := range fv.Data {
					// 		if v == a.V {
					// 			if k != len(fv.Data)-1 {
					// 				fv.Data[k] = fv.Data[len(fv.Data)-1]
					// 			}
					// 			fv.Data = fv.Data[:len(fv.Data)-1]
					// 			break
					// 		}
					// 	}
					// } else if fvlen == 0 {
					// 	delFIeldVal = true
					// }

					fv.Unlock()

					if fvlen == 0 {
						f.Lock()
						delete(f.data, keyValue)
						f.Unlock()
					}
				}
			}
		}

	}

	return
}

func (a *AccesserM) Err() (err error) {
	err = a.err
	return
}

func (a *AccesserM) FilterCount() int {
	return len(a.Filter)
}

func (a *AccesserM) ValueCount() int {
	a.Fv.RLock()
	l := len(a.V.Values)
	a.Fv.RUnlock()
	return l
}

func (a *AccesserM) IsValue() bool {
	if a.V == nil {
		return false
	} else {
		return true
	}
}

type CmapM struct {
	data map[string]*FieldM
	sync.RWMutex
}

func (c *CmapM) Field(field string, key string) (ac *AccesserM) {

	ac = &AccesserM{c: c}

	if field == "" {
		ac.err = errors.New("empty field provided in function Cmap.Field")
		return
	}

	if key == "" {
		ac.err = errors.New("empty key provided in function Cmap.Field")
		return
	}

	c.RLock()
	f, ok := c.data[field]
	c.RUnlock()

	if !ok {

		f := FieldM{}
		f.data = make(map[string]*FieldValueM)

		fv := FieldValueM{}
		fv.Data = make(map[*ValueM]struct{})

		v := ValueM{}
		v.Keys = make(map[string]map[string]struct{})
		v.Keys[field] = make(map[string]struct{})

		//setting
		v.Keys[field][key] = struct{}{}

		fv.Data[&v] = struct{}{}

		f.data[key] = &fv

		c.Lock()
		c.data[field] = &f
		c.Unlock()

		ac.Fv = &fv
		ac.V = &v

		return
	}

	//check fieldData

	f.RLock()
	fv, ok := f.data[key]
	f.RUnlock()

	if !ok {
		fv := FieldValueM{}
		fv.Data = make(map[*ValueM]struct{})

		v := ValueM{}
		v.Keys = make(map[string]map[string]struct{})
		v.Keys[field] = make(map[string]struct{})

		//setting
		v.Keys[field][key] = struct{}{}
		fv.Data[&v] = struct{}{}

		f.Lock()
		f.data[key] = &fv
		f.Unlock()

		ac.Fv = &fv
		ac.V = &v

		return
	} else {
		fv.RLock()
		if len(fv.Data) == 1 {
			for v := range fv.Data {
				ac.V = v
			}
		}
		fv.RUnlock()
	}

	ac.Fv = fv

	return
}

func (c *CmapM) FieldGet(field string, key string) (ac *AccesserM) {

	ac = &AccesserM{c: c}

	if field == "" {
		ac.err = errors.New("empty field provided in function Cmap.Field")
		return
	}

	if key == "" {
		ac.err = errors.New("empty key provided in function Cmap.Field")
		return
	}

	c.RLock()
	f, ok := c.data[field]
	c.RUnlock()

	if !ok {

		return
	}

	//check fieldData

	f.RLock()
	fv, ok := f.data[key]
	f.RUnlock()
	if !ok {
		return
	}

	fv.RLock()
	if len(fv.Data) == 1 {
		for v := range fv.Data {
			ac.V = v
		}
	}
	fv.RUnlock()

	ac.Fv = fv

	return
}

func (c *CmapM) FieldLen(field string) int {
	c.RLock()
	da, ok := c.data[field]
	c.RUnlock()
	if !ok {
		return 0
	}
	da.RLock()
	ret := len(da.data)
	da.RUnlock()
	return ret
}

func (c *CmapM) FieldIterate(field string, fun func(*map[string]*FieldValueM)) {
	c.RLock()
	da, ok := c.data[field]
	c.RUnlock()
	if !ok {
		return
	}
	da.RLock()
	fun(&da.data)
	da.RUnlock()
}

func (c *CmapM) FieldCopy(field string) (ret []string) {
	ret = make([]string, 0)
	c.RLock()
	da, ok := c.data[field]
	c.RUnlock()
	if !ok {
		return
	}
	da.RLock()
	for k := range da.data {
		ret = append(ret, k)
	}
	da.RUnlock()
	return
}

func NewCmapM() *CmapM {
	ret := CmapM{}
	ret.data = make(map[string]*FieldM)
	return &ret
}
