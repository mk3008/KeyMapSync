using System.Text;

namespace KeyMapSync;

public class Result
{
    public string? Table { get; set; } = null;

    public int? Count { get; set; } = null;

    public List<Result>? InnerResults { get; set; } = null;

    private List<Result> GetInnerResults()
    {
        InnerResults ??= new List<Result>();
        return InnerResults;
    }

    internal void Add(Result result)
        => GetInnerResults().Add(result);

    internal void AddRange(List<Result> results)
        => results.ForEach(x => GetInnerResults().Add(x));
}