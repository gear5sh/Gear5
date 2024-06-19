package typeutils

import (
	"sort"

	"github.com/gear5sh/gear5/logger"
	"github.com/gear5sh/gear5/types"
)

type Fields map[string]*Field

type typeNode struct {
	t     types.DataType
	left  *typeNode
	right *typeNode
}

var typecastTree = &typeNode{
	t: types.STRING,
	left: &typeNode{
		t: types.FLOAT64,
		left: &typeNode{
			t:    types.INT64,
			left: &typeNode{t: types.BOOL},
		},
	},
	right: &typeNode{t: types.TIMESTAMP},
}

// Merge adds all fields from other to current instance or merge if exists
func (f Fields) Merge(other Fields) {
	for otherName, otherField := range other {
		if currentField, ok := f[otherName]; ok {
			currentField.Merge(otherField)
			f[otherName] = currentField
		} else {
			f[otherName] = otherField
		}
	}
}

// Clone copies fields into a new Fields object
func (f Fields) Clone() Fields {
	clone := Fields{}

	for fieldName, fieldPayload := range f {
		clonedTypeOccurence := map[types.DataType]bool{}
		for typeName, occurrence := range fieldPayload.typeOccurrence {
			clonedTypeOccurence[typeName] = occurrence
		}

		clone[fieldName] = &Field{
			dataType:       fieldPayload.dataType,
			typeOccurrence: clonedTypeOccurence,
		}
	}

	return clone
}

// OverrideTypes check if field exists in other then put its type
func (f Fields) OverrideTypes(other Fields) {
	for otherName, otherField := range other {
		if currentField, ok := f[otherName]; ok {
			//override type occurrences
			currentField.typeOccurrence = otherField.typeOccurrence
			currentField.dataType = otherField.dataType
			f[otherName] = currentField
		}
	}
}

// Add all new fields from other to current instance
// if field exists - skip it
func (f Fields) Add(other Fields) {
	for otherName, otherField := range other {
		if _, ok := f[otherName]; !ok {
			f[otherName] = otherField
		}
	}
}

// Header return fields names as a string slice
func (f Fields) Header() (header []string) {
	for fieldName := range f {
		header = append(header, fieldName)
	}
	sort.Strings(header)
	return
}

func (f Fields) ToProperties() map[string]*types.Property {
	result := make(map[string]*types.Property)
	for fieldName, field := range f {
		result[fieldName] = &types.Property{
			Type: field.Types(),
		}
	}

	return result
}

// Field is a data type holder with occurrences
type Field struct {
	dataType       *types.DataType
	isNull         bool
	typeOccurrence map[types.DataType]bool
}

// NewField returns Field instance
func NewField(t types.DataType) *Field {
	return &Field{
		dataType:       &t,
		typeOccurrence: map[types.DataType]bool{t: true},
	}
}

// GetType get field type based on occurrence in one file
// lazily get common ancestor type (types.GetCommonAncestorType)
func (f *Field) getType() types.DataType {
	if f.dataType != nil {
		return *f.dataType
	}

	var typs []types.DataType
	for t := range f.typeOccurrence {
		typs = append(typs, t)
	}

	if len(typs) == 0 {
		logger.Fatal("Field typeOccurrence can't be empty")
		return types.UNKNOWN
	}

	common := typs[0]
	for i := 1; i < len(typs); i++ {
		common = GetCommonAncestorType(common, typs[i])
	}

	//put result to dataType (it will be wiped(in Merge) if a new type is added)
	f.dataType = &common
	return common
}

func (f *Field) Types() []types.DataType {
	if f.isNullable() {
		return []types.DataType{types.NULL, f.getType()}
	}
	return []types.DataType{f.getType()}
}

func (f *Field) setNullable() {
	f.isNull = true
}

func (f *Field) isNullable() bool {
	if f.isNull {
		return true
	}

	if _, found := f.typeOccurrence[types.NULL]; found {
		return true
	}

	return false
}

// Merge adds new type occurrences
// wipes field.type if new type was added
func (f *Field) Merge(anotherField *Field) {
	//add new type occurrences
	//wipe field.type if new type was added
	for t := range anotherField.typeOccurrence {
		if _, ok := f.typeOccurrence[t]; !ok {
			f.typeOccurrence[t] = true
			f.dataType = nil
		}
	}
}

// GetCommonAncestorType returns lowest common ancestor type
func GetCommonAncestorType(t1, t2 types.DataType) types.DataType {
	return lowestCommonAncestor(typecastTree, t1, t2)
}

func lowestCommonAncestor(root *typeNode, t1, t2 types.DataType) types.DataType {
	// Start from the root node of the tree
	node := root

	// Traverse the tree
	for node != nil {
		if t1 > node.t && t2 > node.t {
			// If both t1 and t2 are greater than parent
			node = node.right
		} else if t1 < node.t && t2 < node.t {
			// If both t1 and t2 are lesser than parent
			node = node.left
		} else {
			// We have found the split point, i.e. the LCA node.
			return node.t
		}
	}

	return types.UNKNOWN
}
