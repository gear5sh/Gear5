package utils

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	en_translations "github.com/go-playground/validator/v10/translations/en"
	"github.com/sirupsen/logrus"
)

// use a single instance, it caches struct info
var (
	uni      *ut.UniversalTranslator
	validate *validator.Validate
	trans    ut.Translator
)

func translateError(err error) (errs []string) {
	trans, _ := uni.GetTranslator("en")

	if err == nil {
		return nil
	}
	validatorErrs := err.(validator.ValidationErrors)
	for _, e := range validatorErrs {
		translatedErr := fmt.Errorf(e.Translate(trans))
		errs = append(errs, translatedErr.Error())
	}

	return errs
}

func Validate[T any](structure T) error {
	if err := validate.Struct(structure); err != nil {
		return fmt.Errorf(strings.Join(translateError(err), "; "))
	}

	return nil
}

func init() {
	// NOTE: ommitting allot of error checking for brevity
	en := en.New()
	uni = ut.New(en, en)
	trans, _ = uni.GetTranslator("en")

	validate = validator.New(validator.WithRequiredStructEnabled())
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]
		if name == "" {
			name = strings.SplitN(fld.Tag.Get("yaml"), ",", 2)[0]
		}

		fieldName := fld.Name
		if name == "-" {
			return fieldName
		}

		if name != "" {
			return name
		}

		return fieldName
	})

	err := en_translations.RegisterDefaultTranslations(validate, trans)
	if err != nil {
		logrus.Fatal(err)
	}
}
