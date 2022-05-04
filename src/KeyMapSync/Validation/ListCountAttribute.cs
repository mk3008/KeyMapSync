using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KeyMapSync.Validation;

[AttributeUsage(AttributeTargets.Property)]
public sealed class ListRequiredAttribute : ValidationAttribute
{
    public override bool IsValid(object? value) => value is IList lst && lst.Count != 0 ? true : false;

    public override string FormatErrorMessage(string name)
    {
        return $"{name} is requires one or more elements.";
    }
}
