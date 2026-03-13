<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use App\Models\QuickBooksToken;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Crypt;
use Carbon\Carbon;
use Illuminate\Support\Facades\Log;

class QuickBooksReportController extends Controller
{
    private function getBaseUrl()
    {
        return config('qbo.sandbox') ? "https://sandbox-quickbooks.api.intuit.com" : "https://quickbooks.api.intuit.com";
    }


    public function showReportsPage()
    {
        $isConnected = QuickBooksToken::exists();
        return view('reports.select_report', compact('isConnected'));
    }


    public function fetchReport(Request $request, $reportName)
    {

        $startDate = $request->query('start_date', '2025-01-01');
        $endDate   = $request->query('end_date', '2025-12-31');

        $token = QuickBooksToken::first();
        if (!$token) {
            return response()->json(['error' => 'QuickBooks is not connected. Please connect first.'], 401);
        }

        $realmId = Crypt::decryptString($token->realm_id);
        $accessToken = $token->access_token;

        if (Carbon::now()->greaterThan($token->expires_at)) {
            if (!$this->refreshToken()) {
                return response()->json(['error' => 'Failed to refresh token'], 401);
            }
            $token = QuickBooksToken::first();
            $accessToken = $token->access_token;
        }

        $url = $this->getBaseUrl() . "/v3/company/{$realmId}/reports/{$reportName}";

        $response = Http::withHeaders([
            'Authorization' => 'Bearer ' . $accessToken,
            'Accept' => 'application/json'
            ])->get($url, [
                'start_date'            => $startDate,
                'end_date'              => $endDate,

            ]);
        $intuitTid = $response->header('intuit_tid');

        if ($response->failed()) {
            return response()->json([
                'error' => 'Failed to fetch report',
                'details' => $response->json(),
                'intuit_tid' => $intuitTid
            ], 400);
        }

        return response()->json([
            'message' => 'Report fetched successfully',
            'data' => $response->json(),
            'intuit_tid' => $intuitTid
        ]);
    }


    public function fetchReportLive(Request $request)
    {
        $reportName = $request->input('report_name');
        $startDate = $request->input('start_date');
        $endDate = $request->input('end_date');

        $token = QuickBooksToken::first();
        if (!$token) {
            return response()->json(['error' => 'QuickBooks is not connected. Please connect first.'], 401);
        }

        $realmId = Crypt::decryptString($token->realm_id);
        $accessToken = $token->access_token;

        if (Carbon::now()->greaterThan($token->expires_at)) {
            if (!$this->refreshToken()) {
                return response()->json(['error' => 'Failed to refresh token'], 401);
            }
            $token = QuickBooksToken::first();
            $accessToken = $token->access_token;
        }

        $url = $this->getBaseUrl() . "/v3/company/{$realmId}/reports/{$reportName}";

        $queryParams = [];
        if ($startDate) $queryParams['start_date'] = $startDate;
        if ($endDate) $queryParams['end_date'] = $endDate;

        $response = Http::withHeaders([
            'Authorization' => 'Bearer ' . $accessToken,
            'Accept' => 'application/json'
        ])->get($url, $queryParams);

        $intuitTid = $response->header('intuit_tid');

        if ($response->failed()) {
            return response()->json([
                'error' => 'Failed to fetch report',
                'details' => $response->json(),
                'intuit_tid' => $intuitTid
            ], 400);
        }

        return response()->json([
            'message' => 'Report fetched successfully',
            'data' => $response->json(),
            'intuit_tid' => $intuitTid
        ]);
    }

    private function refreshToken()
    {
        $token = QuickBooksToken::first();
        if (!$token || !$token->refresh_token) {
            return false;
        }

        $decryptedRefreshToken = Crypt::decryptString($token->refresh_token);

        $response = Http::asForm()->post('https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer', [
            'grant_type' => 'refresh_token',
            'refresh_token' => $decryptedRefreshToken,
            'client_id' => config('qbo.client_id'),
            'client_secret' => config('qbo.client_secret'),
        ]);

        if ($response->failed()) {
            return false;
        }

        $newToken = $response->json();

        $encryptedRefreshToken = Crypt::encryptString($newToken['refresh_token']);

        $token->update([
            'access_token' => $newToken['access_token'],
            'refresh_token' => $encryptedRefreshToken,
            'expires_at' => Carbon::now()->addSeconds($newToken['expires_in']),
        ]);

        return true;
    }


    public function fetchBalanceSheetFlattened(Request $request)
    {

        $startDate = $request->query('start_date');
        $endDate   = $request->query('end_date');


        $token = QuickBooksToken::first();
        if (!$token) {
            return response()->json(['error' => 'QuickBooks is not connected. Please connect first.'], 401);
        }


    $realmId = Crypt::decryptString($token->realm_id);
    $accessToken = $token->access_token;


    if (Carbon::now()->greaterThan($token->expires_at)) {
        if (!$this->refreshToken()) {
            return response()->json(['error' => 'Failed to refresh token'], 401);
        }
        $token = QuickBooksToken::first();
        $accessToken = $token->access_token;
    }

    $url = $this->getBaseUrl()."/v3/company/{$realmId}/reports/BalanceSheet";
    $response = Http::withHeaders([
        'Authorization' => 'Bearer ' . $accessToken,
        'Accept'        => 'application/json'
    ])->get($url, [
        'start_date'            => $startDate,
        'end_date'              => $endDate,
      //  'summarize_column_by' => 'Total',
    ]);

    if ($response->failed()) {
        return response()->json([
            'error' => 'Failed to fetch report',
            'details' => $response->json()
        ], 400);
    }

    $reportJsonData = $response->json();


    $startPeriod = $reportJsonData['Header']['StartPeriod'] ?? $startDate;
    $year = substr($startPeriod, 0, 4);


    $rows = [];
    if (isset($reportJsonData['Rows'])) {
        $rows = $this->flattenRows(
            $reportJsonData['Rows'],
            [],
            'PNE Pizza LLC',
            $year
        );
    }

    $filename = "Balance_Sheet_Export.csv";
    $headers  = [
        'Content-Type'        => 'text/csv',
        'Content-Disposition' => "attachment; filename=\"$filename\"",
    ];


    return response()->stream(function () use ($rows) {
        $file = fopen('php://output', 'w');


        fputcsv($file, [
            'Company Name',
            'Year',
            'Level 1',
            'Level 2',
            'Level 3',
            'Level 4',
            'Account Name',
            'Value'
        ]);


        foreach ($rows as $row) {
            fputcsv($file, $row);
        }

        fclose($file);
    }, 200, $headers);
}




// Helper function to process rows similar to JavaScript
/**
 * Recursively processes the nested Rows array from QuickBooks.
 * Builds an array of rows; each row has 8 columns:
 * [CompanyName, Year, Level1, Level2, Level3, Level4, AccountName, Value].
 *
 * @param  array  $rows
 * @param  array  $hierarchy
 * @param  string $companyName
 * @param  string $year
 * @return array
 */
private function flattenRows(array $rows, array $hierarchy, string $companyName, string $year)
{
    $result = [];

    // QuickBooks might wrap rows in $rows['Row'], so we expect that:
    if (!isset($rows['Row'])) {
        return $result; // no data
    }

    foreach ($rows['Row'] as $section) {
        // Make a copy of the incoming hierarchy
        $currentHierarchy = $hierarchy;

        // 1) If there's a Header, push it onto the hierarchy
        if (isset($section['Header']['ColData'][0]['value']) && !empty($section['Header']['ColData'][0]['value'])) {
            $currentHierarchy[] = $section['Header']['ColData'][0]['value'];
        }

        // 2) Recurse if there are sub-rows
        if (isset($section['Rows'])) {
            // Merge child rows into our result
            $childFlattened = $this->flattenRows($section['Rows'], $currentHierarchy, $companyName, $year);
            $result = array_merge($result, $childFlattened);
        }

        // 3) If there's ColData, that's an actual "line item"
        if (isset($section['ColData'])) {
            // Make sure we have exactly 4 levels
            while (count($currentHierarchy) < 4) {
                $currentHierarchy[] = '';
            }

            $accountName = $section['ColData'][0]['value'] ?? '';
            $value       = $section['ColData'][1]['value'] ?? '';

            // Build one row of data
            $rowData = [
                $companyName,
                $year,
                $currentHierarchy[0],
                $currentHierarchy[1],
                $currentHierarchy[2],
                $currentHierarchy[3],
                $accountName,
                $value
            ];
            $result[] = $rowData;
        }
    }

    return $result;
}



public function fetchProfitAndLossDetail(Request $request)
{

    $startDate = $request->query('start_date');
    $endDate   = $request->query('end_date');

    $token = QuickBooksToken::first();
    if (!$token) {
        return response()->json(['error' => 'QuickBooks is not connected.'], 401);
    }
    $realmId     = Crypt::decryptString($token->realm_id);
    $accessToken = $token->access_token;



     if (Carbon::now()->greaterThan($token->expires_at)) {
        if (!$this->refreshToken()) {
            return response()->json(['error' => 'Failed to refresh token'], 401);
        }
        $token = QuickBooksToken::first();
        $accessToken = $token->access_token;
    }


    $url = $this->getBaseUrl()."/v3/company/{$realmId}/reports/ProfitAndLossDetail";

    $response = Http::withHeaders([
        'Authorization' => 'Bearer ' . $accessToken,
        'Accept'        => 'application/json'
    ])->get($url, [
        'start_date'           => $startDate,
        'end_date'             => $endDate,
        // Possibly needed: 'accounting_method' => 'Accrual' or 'Cash',
        // Or 'summarize_column_by' => 'Total'
    ]);

    if ($response->failed()) {
        return response()->json([
            'error' => 'Failed to fetch P&L Detail',
            'details' => $response->json(),
        ], 400);
    }

    $reportJsonData = $response->json();

    // 3) Identify how many columns QuickBooks gave us
    //    They appear in ['Columns']['Column'] with 'ColTitle'
    $columns     = $reportJsonData['Columns']['Column'] ?? [];
    $columnCount = count($columns);

    // 4) Build the CSV header row from QuickBooks' column titles.
    //    Example: ["Date", "Transaction Type", "Num", "Name", "Class", "Memo/Description", "Split", "Amount", "Balance"]
    $headerTitles = array_map(function ($col) {
        return $col['ColTitle'] ?? '';
    }, $columns);

    // 5) Recursively flatten *all* rows (Header, ColData, Summary, sub-rows)
    $flattened = [];
    if (isset($reportJsonData['Rows']['Row'])) {
        $this->flattenAllRows($reportJsonData['Rows']['Row'], $flattened, $columnCount);
    }

    // 6) Return CSV
    $filename = "Profit_and_Loss_AllData.csv";
    $headers  = [
        'Content-Type'        => 'text/csv',
        'Content-Disposition' => "attachment; filename=\"$filename\"",
    ];

    return response()->stream(function () use ($headerTitles, $flattened) {
        $file = fopen('php://output', 'w');

        fputcsv($file, $headerTitles);


        foreach ($flattened as $row) {
            fputcsv($file, $row);
        }

        fclose($file);
    }, 200, $headers);
}


/**
 * Recursively processes an array of QBO rows (["Row" => ...]),
 * extracting up to $columnCount columns from Header, ColData, and Summary.
 *
 * @param  array $rows         The array of Row objects from QuickBooks
 * @param  array &$flattened   Reference to our final "flat" array of CSV rows
 * @param  int   $columnCount  How many columns QBO says exist
 */
private function flattenAllRows(array $rows, array &$flattened, int $columnCount)
{
    foreach ($rows as $section) {

        if (isset($section['Header']['ColData'])) {
            $headerRow = $this->colDataToCsvRow($section['Header']['ColData'], $columnCount);
            $flattened[] = $headerRow;
        }


        if (isset($section['ColData'])) {
            $dataRow = $this->colDataToCsvRow($section['ColData'], $columnCount);
            $flattened[] = $dataRow;
        }


        if (isset($section['Summary']['ColData'])) {
            $summaryRow = $this->colDataToCsvRow($section['Summary']['ColData'], $columnCount);
            $flattened[] = $summaryRow;
        }

        // 4) If there are nested rows, recurse
        if (isset($section['Rows']['Row'])) {
            $this->flattenAllRows($section['Rows']['Row'], $flattened, $columnCount);
        }
    }
}

/**
 * Convert an array of QuickBooks-style ColData to exactly $columnCount columns.
 * Example:
 *    Input: [ ["value"=>"2025-01-06"], ["value"=>"Journal Entry"], ... ]
 *    Output (if $columnCount=9): ["2025-01-06", "Journal Entry", "", ..., ""]
 */
private function colDataToCsvRow(array $colData, int $columnCount): array
{
    $row = [];
    for ($i = 0; $i < $columnCount; $i++) {
        $row[] = $colData[$i]['value'] ?? '';
    }
    return $row;
}



public function fetchProfitAndLossDetailall(Request $request)
{
    set_time_limit(0);
    Log::info("Starting fetchProfitAndLossDetailall function.");

    // Accept dynamic date range and chunk size, with safe defaults
    $startDate = $request->query('start_date', '2023-01-01');
    $endDate   = $request->query('end_date', Carbon::now()->toDateString());
    $chunkDays = max(7, min(90, (int) $request->query('chunk_days', 14)));
    $requestGapMs = max(150, min(3000, (int) $request->query('request_gap_ms', 300)));

    Log::info("Parameters: start={$startDate}, end={$endDate}, chunk_days={$chunkDays}, request_gap_ms={$requestGapMs}");

    // 1) Verify or refresh your QuickBooks token
    $token = QuickBooksToken::first();
    if (!$token) {
        Log::error("QuickBooks token not found. Connection issue.");
        return response()->json(['error' => 'QuickBooks is not connected.'], 401);
    }
    Log::info("QuickBooks token found.");

    $realmId     = Crypt::decryptString($token->realm_id);
    $accessToken = $token->access_token;

    if (Carbon::now()->greaterThan($token->expires_at)) {
        Log::warning("QuickBooks token expired. Attempting to refresh...");
        if (!$this->refreshToken()) {
            Log::error("Failed to refresh QuickBooks token.");
            return response()->json(['error' => 'Failed to refresh token'], 401);
        }
        $token       = QuickBooksToken::first();
        $accessToken = $token->access_token;
        Log::info("QuickBooks token refreshed successfully.");
    }

    $urlBase = $this->getBaseUrl() . "/v3/company/{$realmId}/reports/ProfitAndLossDetail";
    Log::info("Base URL for QuickBooks API: " . $urlBase);

    $chunks = $this->generateDateChunks($startDate, $endDate, $chunkDays);
    Log::info("Processing " . count($chunks) . " chunks of {$chunkDays} days each.");

    $allFlattenedRows = [];
    $headerTitles     = [];
    $columnCount      = 0;

    foreach ($chunks as $chunk) {
        Log::info("Fetching chunk: " . $chunk['start_date'] . " -> " . $chunk['end_date']);
        $chunkStart = microtime(true);

        $reportJsonData = $this->fetchChunkWithRetry($urlBase, $accessToken, $chunk, $chunkDays);

        if ($reportJsonData === null) {
            return response()->json([
                'error'   => 'Failed to fetch P&L Detail for chunk ' . $chunk['start_date'] . ' to ' . $chunk['end_date'],
                'details' => 'Chunk failed after retry with smaller date range. Check logs for details.',
            ], 400);
        }

        // Grab column headers once
        if (empty($headerTitles)) {
            $columns     = $reportJsonData['Columns']['Column'] ?? [];
            $columnCount = count($columns);
            $headerTitles = array_map(fn($col) => $col['ColTitle'] ?? '', $columns);
            Log::info("Extracted column headers: " . implode(', ', $headerTitles));
        }

        // Flatten rows for this chunk
        if (isset($reportJsonData['Rows']['Row'])) {
            $flattened = [];
            $this->flattenAllRows($reportJsonData['Rows']['Row'], $flattened, $columnCount);

            foreach ($flattened as &$flatRow) {
                foreach ($flatRow as &$field) {
                    $field = str_replace(["\r", "\n"], ' ', $field);
                }
            }
            unset($flatRow, $field);

            $allFlattenedRows = array_merge($allFlattenedRows, $flattened);
            Log::info("Processed " . count($flattened) . " rows for chunk " . $chunk['start_date'] . " -> " . $chunk['end_date']);
        } else {
            Log::warning("No rows returned for chunk: " . $chunk['start_date'] . " -> " . $chunk['end_date']);
        }

        $elapsed = round(microtime(true) - $chunkStart, 2);
        Log::info("Chunk took {$elapsed}s.");
        $this->paceQuickBooksCalls($requestGapMs);
    }

    Log::info("Total rows collected: " . count($allFlattenedRows));

    $filename   = "Profit_and_Loss_AllData.csv";
    $csvHeaders = [
        'Content-Type'        => 'text/csv',
        'Content-Disposition' => "attachment; filename=\"{$filename}\"",
    ];

    return response()->stream(function () use ($headerTitles, $allFlattenedRows) {
        $file = fopen('php://output', 'w');
        fputcsv($file, $headerTitles);
        foreach ($allFlattenedRows as $row) {
            fputcsv($file, $row);
        }
        fclose($file);
    }, 200, $csvHeaders);
}


/**
 * Generates an array of date-range chunks between $startDate and $endDate,
 * each spanning at most $chunkDays days. The final chunk is clamped to $endDate.
 */
private function generateDateChunks(string $startDate, string $endDate, int $chunkDays): array
{
    $chunks  = [];
    $current = Carbon::parse($startDate);
    $end     = Carbon::parse($endDate);

    while ($current->lte($end)) {
        $chunkEnd = $current->copy()->addDays($chunkDays - 1);
        if ($chunkEnd->gt($end)) {
            $chunkEnd = $end->copy();
        }
        $chunks[] = [
            'start_date' => $current->toDateString(),
            'end_date'   => $chunkEnd->toDateString(),
        ];
        $current->addDays($chunkDays);
    }

    return $chunks;
}

/**
 * ========= NEW: TransactionList CSV (flattened like Balance Sheet) =========
 * Streams a CSV with up to 4 section levels, row type, and all QBO data columns.
 *
 * Query parameters:
 *  - Any official TransactionList params (start_date, end_date, group_by, etc.)
 *  - include_headers (true|false)  -> include section header rows
 *  - include_summaries (true|false)-> include section summary rows
 */
public function exportTransactionListFlattened(Request $request)
{
    // 1) Token lookup
    $token = QuickBooksToken::first();
    if (!$token) {
        return response()->json(['error' => 'QuickBooks is not connected. Please connect first.'], 401);
    }

    $realmId     = Crypt::decryptString($token->realm_id);
    $accessToken = $token->access_token;

    // 2) Refresh if needed
    if (Carbon::now()->greaterThan($token->expires_at)) {
        if (!$this->refreshToken()) {
            return response()->json(['error' => 'Failed to refresh token'], 401);
        }
        $token = QuickBooksToken::first();
        $accessToken = $token->access_token;
    }

    // 3) Allowed TransactionList query params (from docs)
    $allowedParams = [
        'date_macro','payment_method','duedate_macro','arpaid','bothamount','transaction_type',
        'docnum','start_moddate','source_account_type','group_by','start_date','department',
        'start_duedate','columns','end_duedate','vendor','end_date','memo','appaid',
        'moddate_macro','printed','createdate_macro','cleared','customer','qzurl','term',
        'end_createdate','name','sort_by','sort_order','start_createdate','end_moddate'
    ];

    $queryParams = $request->only($allowedParams);

    // Defaults if no explicit date_macro provided
    if (!isset($queryParams['start_date']) && !isset($queryParams['date_macro'])) {
        $queryParams['start_date'] = '2025-01-01';
    }
    if (!isset($queryParams['end_date']) && !isset($queryParams['date_macro'])) {
        $queryParams['end_date'] = '2025-12-31';
    }

    // Output toggles (NOT sent to QBO)
    $includeSummaries = filter_var($request->query('include_summaries', 'true'), FILTER_VALIDATE_BOOLEAN);
    $includeHeaders   = filter_var($request->query('include_headers', 'false'), FILTER_VALIDATE_BOOLEAN);

    // 4) Call QBO
    $url = $this->getBaseUrl() . "/v3/company/{$realmId}/reports/TransactionList";

    $response = Http::withHeaders([
        'Authorization' => 'Bearer ' . $accessToken,
        'Accept'        => 'application/json'
    ])->get($url, $queryParams);

    $intuitTid = $response->header('intuit_tid');

    if ($response->failed()) {
        return response()->json([
            'error'      => 'Failed to fetch TransactionList',
            'details'    => $response->json(),
            'intuit_tid' => $intuitTid
        ], 400);
    }

    $report = $response->json();

    // 5) QBO dynamic columns
    $columns     = $report['Columns']['Column'] ?? [];
    $columnCount = count($columns);
    $dataHeaders = array_map(fn($c) => $c['ColTitle'] ?? '', $columns);

    // 6) CSV header: section levels + row type + QBO columns
    $csvHeader = array_merge(
        ['Group Level 1', 'Group Level 2', 'Group Level 3', 'Group Level 4', 'Row Type'],
        $dataHeaders
    );

    // 7) Flatten rows
    $flat = [];
    if (isset($report['Rows'])) {
        $this->flattenTransactionListRows(
            $report['Rows'],
            [],
            $columnCount,
            $flat,
            $includeHeaders,
            $includeSummaries
        );
    }

    // 8) Stream CSV
    $filename = "Transaction_List_Flattened.csv";
    $headers  = [
        'Content-Type'        => 'text/csv',
        'Content-Disposition' => "attachment; filename=\"{$filename}\"",
        'intuit_tid'          => $intuitTid ?? ''
    ];

    return response()->stream(function () use ($csvHeader, $flat) {
        $out = fopen('php://output', 'w');
        fputcsv($out, $csvHeader);

        foreach ($flat as $row) {
            foreach ($row as &$cell) {
                if (is_string($cell)) {
                    $cell = str_replace(["\r", "\n"], ' ', $cell);
                }
            }
            fputcsv($out, $row);
        }

        fclose($out);
    }, 200, $headers);
}

/**
 * Recursively flattens the nested "Rows" structure of a TransactionList report.
 * Outputs: [Level1, Level2, Level3, Level4, RowType, <QBO ColData...>]
 * RowType ∈ {"Header","Data","Summary"}; headers/summaries optional via flags.
 */
private function flattenTransactionListRows(
    array $rowsNode,
    array $hierarchy,
    int $columnCount,
    array &$out,
    bool $includeHeaders = false,
    bool $includeSummaries = true
): void {
    if (!isset($rowsNode['Row'])) {
        return;
    }

    foreach ($rowsNode['Row'] as $node) {
        $currentHierarchy = $hierarchy;

        // Section header -> extend hierarchy with first ColData value
        if (isset($node['Header']['ColData'][0]['value']) && $node['Header']['ColData'][0]['value'] !== '') {
            $currentHierarchy[] = $node['Header']['ColData'][0]['value'];

            if ($includeHeaders) {
                $levels = $this->padLevels($currentHierarchy, 4);
                $out[]  = array_merge($levels, ['Header'], array_fill(0, $columnCount, ''));
            }
        }

        // Nested rows
        if (isset($node['Rows'])) {
            $this->flattenTransactionListRows($node['Rows'], $currentHierarchy, $columnCount, $out, $includeHeaders, $includeSummaries);
        }

        // Data line
        if (isset($node['ColData'])) {
            $levels  = $this->padLevels($currentHierarchy, 4);
            $dataRow = $this->colDataToCsvRow($node['ColData'], $columnCount);
            $out[]   = array_merge($levels, ['Data'], $dataRow);
        }

        // Summary line
        if ($includeSummaries && isset($node['Summary']['ColData'])) {
            $levels     = $this->padLevels($currentHierarchy, 4);
            $summaryRow = $this->colDataToCsvRow($node['Summary']['ColData'], $columnCount);
            $out[]      = array_merge($levels, ['Summary'], $summaryRow);
        }
    }
}

/**
 * Pads an array of hierarchy levels to exactly $n items.
 */
private function padLevels(array $levels, int $n): array
{
    while (count($levels) < $n) {
        $levels[] = '';
    }
    return array_slice($levels, 0, $n);
}

/**
 * Adds a small delay between QuickBooks calls to avoid per-second burst limits.
 */
private function paceQuickBooksCalls(int $delayMs = 300): void
{
    usleep(max(0, $delayMs) * 1000);
}

/**
 * Performs a resilient GET to QuickBooks with backoff for throttling and transient failures.
 */
private function qboGetWithBackoff(string $url, string $accessToken, array $queryParams, int $maxAttempts = 6): ?\Illuminate\Http\Client\Response
{
    for ($attempt = 1; $attempt <= $maxAttempts; $attempt++) {
        try {
            $response = Http::withHeaders([
                'Authorization' => 'Bearer ' . $accessToken,
                'Accept'        => 'application/json',
            ])->withOptions([
                'http_errors' => false,
                'version' => 1.1,
                'connect_timeout' => 20,
                'timeout' => 180,
            ])->get($url, $queryParams);
        } catch (\Throwable $e) {
            if ($attempt === $maxAttempts) {
                Log::error('QBO request failed after max attempts due to transport error.', [
                    'attempts' => $maxAttempts,
                    'message' => $e->getMessage(),
                    'url' => $url,
                    'query' => $queryParams,
                ]);
                return null;
            }

            $delayMs = $this->calculateBackoffDelayMs($attempt);
            Log::warning('Transient transport error calling QBO; retrying.', [
                'attempt' => $attempt,
                'max_attempts' => $maxAttempts,
                'delay_ms' => $delayMs,
                'message' => $e->getMessage(),
            ]);
            $this->paceQuickBooksCalls($delayMs);
            continue;
        }

        if ($response->successful()) {
            return $response;
        }

        $status = $response->status();
        $retryable = in_array($status, [429, 500, 502, 503, 504], true);

        if (!$retryable || $attempt === $maxAttempts) {
            return $response;
        }

        $retryAfterSeconds = $this->parseRetryAfterSeconds($response->header('Retry-After'));
        $delayMs = $retryAfterSeconds !== null
            ? max(1000, $retryAfterSeconds * 1000)
            : $this->calculateBackoffDelayMs($attempt);

        Log::warning('QBO returned retryable status; backing off.', [
            'attempt' => $attempt,
            'max_attempts' => $maxAttempts,
            'status' => $status,
            'delay_ms' => $delayMs,
            'intuit_tid' => $response->header('intuit_tid'),
        ]);

        $this->paceQuickBooksCalls($delayMs);
    }

    return null;
}

private function calculateBackoffDelayMs(int $attempt): int
{
    $base = min(15000, 500 * (2 ** max(0, $attempt - 1)));
    $jitter = random_int(100, 600);
    return (int) ($base + $jitter);
}

private function parseRetryAfterSeconds($retryAfterHeader): ?int
{
    if (empty($retryAfterHeader)) {
        return null;
    }

    if (is_numeric($retryAfterHeader)) {
        return max(0, (int) $retryAfterHeader);
    }

    try {
        $retryAt = Carbon::parse($retryAfterHeader);
        return max(0, Carbon::now()->diffInSeconds($retryAt, false));
    } catch (\Throwable $e) {
        return null;
    }
}

/**
 * Fetches a single P&L chunk from the QBO API.
 * On failure, splits the chunk in half and retries each sub-chunk after a 2-second pause.
 * Returns the merged report JSON on success, or null if all retries fail.
 */
private function fetchChunkWithRetry(string $url, string $accessToken, array $chunk, int $chunkDays): ?array
{
    $response = $this->qboGetWithBackoff($url, $accessToken, [
        'start_date' => $chunk['start_date'],
        'end_date'   => $chunk['end_date'],
    ]);

    if ($response === null) {
        return null;
    }

    if ($response->successful()) {
        return $response->json();
    }

    Log::warning(
        "Chunk {$chunk['start_date']} - {$chunk['end_date']} failed (HTTP {$response->status()}). "
        . "Retrying with half-size sub-chunks."
    );

    $halfDays  = max(7, (int) ceil($chunkDays / 2));
    $subChunks = $this->generateDateChunks($chunk['start_date'], $chunk['end_date'], $halfDays);

    $mergedData = null;

    foreach ($subChunks as $sub) {
        $this->paceQuickBooksCalls(1200);

        $subResponse = $this->qboGetWithBackoff($url, $accessToken, [
            'start_date' => $sub['start_date'],
            'end_date'   => $sub['end_date'],
        ]);

        if ($subResponse === null) {
            Log::error(
                "Sub-chunk {$sub['start_date']} - {$sub['end_date']} failed due to transport-level failure after retries."
            );
            return null;
        }

        if ($subResponse->failed()) {
            Log::error(
                "Sub-chunk {$sub['start_date']} - {$sub['end_date']} also failed (HTTP {$subResponse->status()}).",
                ['details' => $subResponse->json()]
            );
            return null;
        }

        $subData = $subResponse->json();

        if ($mergedData === null) {
            $mergedData = $subData;
        } else {
            // Append rows from subsequent sub-chunks into the merged result
            $newRows = $subData['Rows']['Row'] ?? [];
            if (!empty($newRows)) {
                $mergedData['Rows']['Row'] = array_merge(
                    $mergedData['Rows']['Row'] ?? [],
                    $newRows
                );
            }
        }
    }

    return $mergedData;
}

/**
 * ========= NEW: JournalReport CSV (flattened like TransactionList) =========
 * Streams a CSV with up to 4 group levels, row type, and all QBO data columns.
 *
 * Query parameters (sent to QBO):
 *  - start_date, end_date, date_macro, sort_by, sort_order, columns
 *
 * Output toggles (NOT sent to QBO):
 *  - include_headers (true|false)   -> include section header rows if present
 *  - include_summaries (true|false) -> include summary rows
 */
public function exportJournalReportFlattened(Request $request)
{
    // 1) Token lookup
    $token = QuickBooksToken::first();
    if (!$token) {
        return response()->json(['error' => 'QuickBooks is not connected. Please connect first.'], 401);
    }

    $realmId     = Crypt::decryptString($token->realm_id);
    $accessToken = $token->access_token;

    // 2) Refresh if needed
    if (Carbon::now()->greaterThan($token->expires_at)) {
        if (!$this->refreshToken()) {
            return response()->json(['error' => 'Failed to refresh token'], 401);
        }
        $token = QuickBooksToken::first();
        $accessToken = $token->access_token;
    }

    // 3) Allowed JournalReport query params (from docs)
    $allowedParams = [
        'start_date',
        'end_date',
        'date_macro',
        'sort_by',
        'sort_order',
        'columns',
    ];

    $queryParams = $request->only($allowedParams);

    // Defaults if no explicit date_macro provided
    if (!isset($queryParams['start_date']) && !isset($queryParams['date_macro'])) {
        $queryParams['start_date'] = '2025-01-01';
    }
    if (!isset($queryParams['end_date']) && !isset($queryParams['date_macro'])) {
        $queryParams['end_date'] = '2025-12-31';
    }

    // Output toggles (NOT sent to QBO)
    $includeSummaries = filter_var($request->query('include_summaries', 'true'), FILTER_VALIDATE_BOOLEAN);
    $includeHeaders   = filter_var($request->query('include_headers', 'false'), FILTER_VALIDATE_BOOLEAN);

    // 4) Call QBO
    $url = $this->getBaseUrl() . "/v3/company/{$realmId}/reports/JournalReport";

    $response = Http::withHeaders([
        'Authorization' => 'Bearer ' . $accessToken,
        'Accept'        => 'application/json'
    ])->get($url, $queryParams);

    $intuitTid = $response->header('intuit_tid');

    if ($response->failed()) {
        return response()->json([
            'error'      => 'Failed to fetch JournalReport',
            'details'    => $response->json(),
            'intuit_tid' => $intuitTid
        ], 400);
    }

    $report = $response->json();

    // 5) QBO dynamic columns
    $columns     = $report['Columns']['Column'] ?? [];
    $columnCount = count($columns);
    $dataHeaders = array_map(fn($c) => $c['ColTitle'] ?? '', $columns);

    // 6) CSV header: group levels + row type + QBO columns
    $csvHeader = array_merge(
        ['Group Level 1', 'Group Level 2', 'Group Level 3', 'Group Level 4', 'Row Type'],
        $dataHeaders
    );

    // 7) Flatten rows
    $flat = [];
    if (isset($report['Rows'])) {
        $this->flattenJournalReportRows(
            $report['Rows'],
            [],
            $columnCount,
            $flat,
            $includeHeaders,
            $includeSummaries
        );
    }

    // 8) Stream CSV
    $filename = "Journal_Report_Flattened.csv";
    $headers  = [
        'Content-Type'        => 'text/csv',
        'Content-Disposition' => "attachment; filename=\"{$filename}\"",
        'intuit_tid'          => $intuitTid ?? ''
    ];

    return response()->stream(function () use ($csvHeader, $flat) {
        $out = fopen('php://output', 'w');
        fputcsv($out, $csvHeader);

        foreach ($flat as $row) {
            foreach ($row as &$cell) {
                if (is_string($cell)) {
                    $cell = str_replace(["\r", "\n"], ' ', $cell);
                }
            }
            fputcsv($out, $row);
        }

        fclose($out);
    }, 200, $headers);
}

/**
 * Recursively flattens the nested "Rows" structure of a JournalReport.
 * Outputs: [Level1, Level2, Level3, Level4, RowType, <QBO ColData...>]
 * RowType ∈ {"Header","Data","Summary"}; headers/summaries optional via flags.
 *
 * Note: JournalReport is usually flat (no nested groups), but this safely handles
 * Section headers, summaries, and nested Rows if Intuit returns them.
 */
private function flattenJournalReportRows(
    array $rowsNode,
    array $hierarchy,
    int $columnCount,
    array &$out,
    bool $includeHeaders = false,
    bool $includeSummaries = true
): void {
    if (!isset($rowsNode['Row'])) {
        return;
    }

    foreach ($rowsNode['Row'] as $node) {
        $currentHierarchy = $hierarchy;

        // If Intuit returns a Section Header (rare for JournalReport),
        // use it as a group level like TransactionList.
        if (isset($node['Header']['ColData'][0]['value']) && $node['Header']['ColData'][0]['value'] !== '') {
            $currentHierarchy[] = $node['Header']['ColData'][0]['value'];

            if ($includeHeaders) {
                $levels = $this->padLevels($currentHierarchy, 4);
                $out[]  = array_merge($levels, ['Header'], array_fill(0, $columnCount, ''));
            }
        }

        // Nested rows (just in case)
        if (isset($node['Rows'])) {
            $this->flattenJournalReportRows(
                $node['Rows'],
                $currentHierarchy,
                $columnCount,
                $out,
                $includeHeaders,
                $includeSummaries
            );
        }

        // Data lines (JournalReport rows are mostly type=Data with ColData)
        if (isset($node['ColData'])) {
            $levels  = $this->padLevels($currentHierarchy, 4);
            $dataRow = $this->colDataToCsvRow($node['ColData'], $columnCount);
            $out[]   = array_merge($levels, ['Data'], $dataRow);
        }

        // Summary lines typically appear on Section nodes
        if ($includeSummaries && isset($node['Summary']['ColData'])) {
            $levels     = $this->padLevels($currentHierarchy, 4);
            $summaryRow = $this->colDataToCsvRow($node['Summary']['ColData'], $columnCount);
            $out[]      = array_merge($levels, ['Summary'], $summaryRow);
        }
    }
}

}
