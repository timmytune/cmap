package cmap

import (
	"errors"
	"sync"
)

type Valuem struct {
	Keys   map[string]map[string]struct{}
	Values map[string]interface{}
	sync.RWMutex
}

type FieldValuem struct {
	Data []*Valuem

	sync.RWMutex
}

type Fieldm struct {
	data map[string]*FieldValuem
	sync.RWMutex
}

type Accesserm struct {
	c      *Cmapm
	V      *Valuem
	Fv     *FieldValuem
	Filter []*Valuem
	err    error
}

func (a *Accesserm) Set(field string, key string) (ac *Accesserm) {
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

		f := Fieldm{}
		f.data = make(map[string]*FieldValuem)

		fv := FieldValuem{}
		fv.Data = make([]*Valuem, 0)

		//setting
		a.V.Lock()
		if a.V.Keys[field] == nil {
			a.V.Keys[field] = make(map[string]struct{})
		}
		a.V.Keys[field][key] = struct{}{}
		a.V.Unlock()

		fv.Data = append(fv.Data, a.V)

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
		fv := FieldValuem{}
		fv.Data = make([]*Valuem, 0)

		//setting
		a.V.Lock()
		if a.V.Keys[field] == nil {
			a.V.Keys[field] = make(map[string]struct{})
		}
		a.V.Keys[field][key] = struct{}{}
		a.V.Unlock()

		fv.Data = append(fv.Data, a.V)

		f.Lock()
		f.data[key] = &fv
		f.Unlock()

		a.Fv = &fv

		return
	}

	vInFv := false

	fv.Lock()
	for _, v1 := range fv.Data {
		if v1 == a.V {
			vInFv = true
			break
		}
	}
	if !vInFv {

		a.V.Lock()
		if a.V.Keys[field] == nil {
			a.V.Keys[field] = make(map[string]struct{})
		}
		a.V.Keys[field][key] = struct{}{}
		a.V.Unlock()

		fv.Data = append(fv.Data, a.V)
	}
	fv.Unlock()

	a.Fv = fv

	return
}

func (a *Accesserm) FilterKV(f map[string]string) (ac *Accesserm) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make([]*Valuem, 0)

	if len(a.Filter) > 0 {
		for _, v := range a.Filter {

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
				filter = append(filter, v)
			}
		}
	} else {
		a.Fv.RLock()
		for _, v := range a.Fv.Data {

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
				filter = append(filter, v)
			}
		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		a.V = filter[0]
		a.Filter = filter
	}

	return
}

func (a *Accesserm) FilterNoKey(key string) (ac *Accesserm) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make([]*Valuem, 0)

	if len(a.Filter) > 0 {
		for _, v := range a.Filter {

			v.RLock()
			_, ok := v.Keys[key]
			v.RUnlock()
			if !ok {
				filter = append(filter, v)
			}

		}
	} else {
		a.Fv.RLock()
		for _, v := range a.Fv.Data {
			v.RLock()
			_, ok := v.Keys[key]
			v.RUnlock()
			if !ok {
				filter = append(filter, v)
			}
		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		a.V = filter[0]
		a.Filter = filter
	}

	return
}

func (a *Accesserm) FilterHasKey(key string) (ac *Accesserm) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make([]*Valuem, 0)

	if len(a.Filter) > 0 {
		for _, v := range a.Filter {

			v.RLock()
			_, ok := v.Keys[key]
			v.RUnlock()
			if ok {
				filter = append(filter, v)
			}

		}
	} else {
		a.Fv.RLock()
		for _, v := range a.Fv.Data {
			v.RLock()
			_, ok := v.Keys[key]
			v.RUnlock()
			if ok {
				filter = append(filter, v)
			}
		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		a.V = filter[0]
		a.Filter = filter
	}

	return
}

func (a *Accesserm) FilterHasValue(key string) (ac *Accesserm) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make([]*Valuem, 0)

	if len(a.Filter) > 0 {
		for _, v := range a.Filter {

			v.Lock()
			if v.Values == nil {
				v.Values = make(map[string]interface{})
			}
			_, ok := v.Values[key]
			v.Unlock()
			if ok {
				filter = append(filter, v)
			}

		}
	} else {
		a.Fv.RLock()
		for _, v := range a.Fv.Data {

			v.Lock()
			if v.Values == nil {
				v.Values = make(map[string]interface{})
			}
			_, ok := v.Values[key]
			v.Unlock()
			if ok {
				filter = append(filter, v)
			}

		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		a.V = filter[0]
		a.Filter = filter
	}

	return
}

func (a *Accesserm) FilterNotValue(key string) (ac *Accesserm) {
	ac = a

	if a.err != nil {
		return
	}

	filter := make([]*Valuem, 0)

	if len(a.Filter) > 0 {
		for _, v := range a.Filter {

			v.Lock()
			if v.Values == nil {
				v.Values = make(map[string]interface{})
			}
			_, ok := v.Values[key]
			v.Unlock()
			if !ok {
				filter = append(filter, v)
			}

		}
	} else {
		a.Fv.RLock()
		for _, v := range a.Fv.Data {

			v.Lock()
			if v.Values == nil {
				v.Values = make(map[string]interface{})
			}
			_, ok := v.Values[key]
			v.Unlock()
			if !ok {
				filter = append(filter, v)
			}

		}
		a.Fv.Unlock()
	}

	if len(filter) > 1 {
		a.Filter = filter
	} else if len(filter) == 0 {
		a.err = errors.New("no value matched filter")
	} else if len(filter) == 1 {
		a.V = filter[0]
		a.Filter = filter
	}

	return
}

func (a *Accesserm) SetValue(key string, val interface{}) (ac *Accesserm) {
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

func (a *Accesserm) GetValue(key string) (val interface{}, err error) {

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

func (a *Accesserm) GetValueString(key string) (vaa string, err error) {

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

func (a *Accesserm) GetField(key string) (val map[string]struct{}, err error) {

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

func (a *Accesserm) DeleteValue(key string) (err error) {

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

func (a *Accesserm) DeleteField(field string) (err error) {
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

	fvs := make(map[string]*FieldValuem)

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

		del := false

		v.Lock()

		fvlen := len(v.Data)

		if fvlen == 1 && v.Data[0] == a.V {
			del = true
		} else if fvlen == 1 && v.Data[0] != a.V {
			a.err = errors.New("fieldvalue with this key does not have the current value attached ")
			err = a.err
		} else if fvlen > 1 {
			for k2, v2 := range v.Data {
				if v2 == a.V {
					if k2 != len(v.Data)-1 {
						v.Data[k2] = v.Data[len(v.Data)-1]
					}
					v.Data = v.Data[:len(v.Data)-1]
					break
				}
			}
		} else if fvlen == 0 {
			del = true
		}

		v.Unlock()

		if del {
			f.Lock()
			delete(f.data, k)
			f.Unlock()
		}

	}

	return
}

func (a *Accesserm) DeleteFieldValue(field string, value string) (err error) {
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

	var v *FieldValuem

	f.RLock()
	v, ok = f.data[value]
	f.RUnlock()

	if !ok {
		return
	}

	del := false

	v.Lock()

	fvlen := len(v.Data)

	if fvlen == 1 && v.Data[0] == a.V {
		del = true
	} else if fvlen == 1 && v.Data[0] != a.V {
		a.err = errors.New("fieldvalue with this key does not have the current value attached ")
		err = a.err
	} else if fvlen > 1 {
		for k2, v2 := range v.Data {
			if v2 == a.V {
				if k2 != len(v.Data)-1 {
					v.Data[k2] = v.Data[len(v.Data)-1]
				}
				v.Data = v.Data[:len(v.Data)-1]
				break
			}
		}
	} else if fvlen == 0 {
		del = true
	}

	v.Unlock()

	if del {
		f.Lock()
		delete(f.data, value)
		f.Unlock()
	}

	return
}

func (a *Accesserm) Delete() (err error) {

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

					delFIeldVal := false

					fv.Lock()

					fvlen := len(fv.Data)

					if fvlen == 1 && fv.Data[0] == a.V {
						delFIeldVal = true
					} else if fvlen == 1 && fv.Data[0] != a.V {
						a.err = errors.New("fieldvalue with this key" + keyValue + " does not have the current value attached ")
						err = a.err
					} else if fvlen > 1 {
						for k, v := range fv.Data {
							if v == a.V {
								if k != len(fv.Data)-1 {
									fv.Data[k] = fv.Data[len(fv.Data)-1]
								}
								fv.Data = fv.Data[:len(fv.Data)-1]
								break
							}
						}
					} else if fvlen == 0 {
						delFIeldVal = true
					}

					fv.Unlock()

					if delFIeldVal {
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

func (a *Accesserm) Err() (err error) {
	err = a.err
	return
}

func (a *Accesserm) FilterCount() int {
	return len(a.Filter)
}

func (a *Accesserm) ValueCount() int {
	a.Fv.RLock()
	l := len(a.V.Values)
	a.Fv.RUnlock()
	return l
}

func (a *Accesserm) IsValue() bool {
	if a.V == nil {
		return false
	} else {
		return true
	}
}

type Cmapm struct {
	data map[string]*Fieldm
	sync.RWMutex
}

func (c *Cmapm) Field(field string, key string) (ac *Accesserm) {

	ac = &Accesserm{c: c}

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

		f := Fieldm{}
		f.data = make(map[string]*FieldValuem)

		fv := FieldValuem{}
		fv.Data = make([]*Valuem, 0)

		v := Valuem{}
		v.Keys = make(map[string]map[string]struct{})
		v.Keys[field] = make(map[string]struct{})

		//setting
		v.Keys[field][key] = struct{}{}

		fv.Data = append(fv.Data, &v)

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
		fv := FieldValuem{}
		fv.Data = make([]*Valuem, 0)

		v := Valuem{}
		v.Keys = make(map[string]map[string]struct{})
		v.Keys[field] = make(map[string]struct{})

		//setting
		v.Keys[field][key] = struct{}{}
		fv.Data = append(fv.Data, &v)

		f.Lock()
		f.data[key] = &fv
		f.Unlock()

		ac.Fv = &fv
		ac.V = &v

		return
	} else {
		fv.RLock()
		if len(fv.Data) == 1 {
			v := fv.Data[0]
			ac.V = v
		}
		fv.RUnlock()
	}

	ac.Fv = fv

	return
}

func (c *Cmapm) FieldLen(field string) int {
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

func (c *Cmapm) FieldIterate(field string, fun func(*map[string]*FieldValuem)) {
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

func (c *Cmapm) FieldCopy(field string) (ret []string) {
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

func NewCmapm() *Cmapm {
	ret := Cmapm{}
	ret.data = make(map[string]*Fieldm)
	return &ret
}
